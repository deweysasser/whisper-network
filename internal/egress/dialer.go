package egress

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"
)

// ErrRefused is returned when every address for a target is filtered.
var ErrRefused = errors.New("egress: address refused")

// Resolver is the lookup surface the Dialer needs; *net.Resolver satisfies it.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// Dialer resolves, filters, and dials the checked IP literal.
type Dialer struct {
	AllowPrivate bool
	Resolver     Resolver // nil means net.DefaultResolver
	Inner        net.Dialer
}

// DialContext resolves addr once, drops refused addresses (unless AllowPrivate)
// and dials the remaining IP literals in order. There is no second resolution.
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
// goes through the filter. It never uses environment proxies. Callers should
// reuse the returned client (it pools connections; idle ones expire after 30s).
//
// Errors returned by the client are *url.Error values containing the full
// URL; callers must not log them verbatim (log the site key instead).
func (d *Dialer) HTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:                 nil,
			DialContext:           d.DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			IdleConnTimeout:       30 * time.Second,
			MaxIdleConns:          16,
			ResponseHeaderTimeout: timeout,
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
