package egress

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ErrRefused is returned when every address for a target is filtered, or its
// port is not allowed.
var ErrRefused = errors.New("egress: address refused")

// DefaultPorts are the TCP ports a Dialer allows when AllowPorts is nil.
var DefaultPorts = []uint16{80, 443}

// Timeouts of HTTPClient beyond its overall one.
const (
	// getHeaderTimeout bounds the wait for a response's header.
	getHeaderTimeout = time.Minute
	// getIdleTimeout is the longest a connection may go without a byte
	// read or written, as the proxy's idleTimeout.
	getIdleTimeout = idleTimeout
)

// Resolver is the lookup surface the Dialer needs; *net.Resolver satisfies it.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// Dialer resolves, filters, and dials the checked IP literal.
type Dialer struct {
	AllowPrivate bool
	// AllowPorts are the only TCP ports it dials; nil means DefaultPorts.
	// The proxy (CONNECT and forwarded GETs) and HTTPClient (the plain GET
	// and every redirect hop) share it.
	AllowPorts []uint16
	Resolver   Resolver // nil means net.DefaultResolver
	Inner      net.Dialer
}

// PortAllowed reports whether d dials TCP port p.
func (d *Dialer) PortAllowed(p uint16) bool {
	ports := d.AllowPorts
	if ports == nil {
		ports = DefaultPorts
	}
	return slices.Contains(ports, p)
}

// WithPorts returns a copy of d that also allows ports.
func (d *Dialer) WithPorts(ports ...uint16) *Dialer {
	e := *d
	base := d.AllowPorts
	if base == nil {
		base = DefaultPorts
	}
	e.AllowPorts = slices.Concat(base, ports)
	return &e
}

// DialContext resolves addr once, drops refused addresses (unless AllowPrivate)
// and dials the remaining IP literals in order. There is no second resolution.
// A port outside AllowPorts is refused before any lookup.
func (d *Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	switch network {
	case "tcp", "tcp4", "tcp6":
	default:
		return nil, fmt.Errorf("egress: unsupported network %q", network)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("egress: bad port %q", port)
	}
	if !d.PortAllowed(uint16(n)) {
		return nil, fmt.Errorf("%w: port %d", ErrRefused, n)
	}
	var addrs []netip.Addr
	if ip, perr := netip.ParseAddr(host); perr == nil {
		addrs = []netip.Addr{ip}
	} else {
		var r Resolver = net.DefaultResolver
		if d.Resolver != nil {
			r = d.Resolver
		}
		lookupNet := "ip"
		if network == "tcp4" {
			lookupNet = "ip4"
		} else if network == "tcp6" {
			lookupNet = "ip6"
		}
		addrs, err = r.LookupNetIP(ctx, lookupNet, host)
		if err != nil {
			return nil, err
		}
	}
	var lastErr error
	for _, a := range addrs {
		if !d.AllowPrivate && Refused(a) {
			continue
		}
		c, err := d.Inner.DialContext(ctx, network, net.JoinHostPort(a.WithZone("").String(), port))
		if err == nil {
			return c, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("%w: %s", ErrRefused, host)
}

// HTTPClient returns a client whose every connection (including redirect hops)
// goes through the filter, ports included. It never uses environment proxies.
// timeout bounds a whole request, body included; the response header must
// arrive within a minute (or timeout, if shorter), and a connection that moves
// no byte for 2 minutes is closed. Callers should reuse the returned client
// (it pools connections; idle ones expire after 30s).
//
// Errors returned by the client are *url.Error values containing the full
// URL; callers must not log them verbatim (log the site key instead).
func (d *Dialer) HTTPClient(timeout time.Duration) *http.Client {
	return d.httpClient(timeout, min(timeout, getHeaderTimeout), getIdleTimeout)
}

func (d *Dialer) httpClient(timeout, header, idle time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy: nil,
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				c, err := d.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return &idleConn{Conn: c, idle: idle}, nil
			},
			TLSHandshakeTimeout:   10 * time.Second,
			IdleConnTimeout:       30 * time.Second,
			MaxIdleConns:          16,
			ResponseHeaderTimeout: header,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("egress: stopped after 10 redirects")
			}
			if s := strings.ToLower(req.URL.Scheme); s != "http" && s != "https" {
				return fmt.Errorf("egress: redirect to scheme %q refused", req.URL.Scheme)
			}
			return nil
		},
	}
}

// idleConn is a connection whose every read and write must make progress
// within idle: each call pushes the deadline out by idle.
type idleConn struct {
	net.Conn
	idle time.Duration
}

func (c *idleConn) Read(p []byte) (int, error) {
	_ = c.Conn.SetDeadline(time.Now().Add(c.idle))
	return c.Conn.Read(p)
}

func (c *idleConn) Write(p []byte) (int, error) {
	_ = c.Conn.SetDeadline(time.Now().Add(c.idle))
	return c.Conn.Write(p)
}
