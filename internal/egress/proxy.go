package egress

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Proxy is a loopback HTTP proxy that dials only through a filtering Dialer.
type Proxy struct {
	ln     net.Listener
	d      *Dialer
	cfg    proxyConfig
	ctx    context.Context
	cancel context.CancelFunc
	sem    chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

type proxyConfig struct {
	allowPort func(uint16) bool
	header    time.Duration // time allowed to receive the request header
	idle      time.Duration // max inactivity (either direction)
	maxConns  int
}

const (
	headerTimeout = 15 * time.Second
	idleTimeout   = 2 * time.Minute
	maxHeader     = 64 << 10
	maxClientConn = 64
)

func defaultAllowPort(p uint16) bool { return p == 80 || p == 443 }

// StartProxy listens on 127.0.0.1:0 and serves until ctx is done or Close.
func StartProxy(ctx context.Context, d *Dialer) (*Proxy, error) {
	return startProxyCfg(ctx, d, proxyConfig{})
}

func startProxy(ctx context.Context, d *Dialer, allow func(uint16) bool) (*Proxy, error) {
	return startProxyCfg(ctx, d, proxyConfig{allowPort: allow})
}

func startProxyCfg(ctx context.Context, d *Dialer, cfg proxyConfig) (*Proxy, error) {
	if cfg.allowPort == nil {
		cfg.allowPort = defaultAllowPort
	}
	if cfg.header == 0 {
		cfg.header = headerTimeout
	}
	if cfg.idle == 0 {
		cfg.idle = idleTimeout
	}
	if cfg.maxConns == 0 {
		cfg.maxConns = maxClientConn
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	pctx, cancel := context.WithCancel(ctx)
	p := &Proxy{ln: ln, d: d, cfg: cfg, ctx: pctx, cancel: cancel,
		sem: make(chan struct{}, cfg.maxConns), conns: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go p.serve()
	go func() { <-pctx.Done(); p.Close() }()
	return p, nil
}

// Port is the listening TCP port.
func (p *Proxy) Port() uint16 { return uint16(p.ln.Addr().(*net.TCPAddr).Port) }

// Close stops the listener, drops live connections and waits for handlers.
func (p *Proxy) Close() error {
	p.cancel()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	err := p.ln.Close()
	for c := range p.conns {
		c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
	return err
}

func (p *Proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *Proxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
}

func transientAccept(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.ENOBUFS) ||
		errors.Is(err, syscall.ENOMEM)
}

func (p *Proxy) serve() {
	defer p.wg.Done()
	delay := 5 * time.Millisecond
	for {
		c, err := p.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if transientAccept(err) {
				select {
				case <-time.After(delay):
				case <-p.ctx.Done():
					return
				}
				if delay *= 2; delay > time.Second {
					delay = time.Second
				}
				continue
			}
			p.ln.Close()
			return
		}
		delay = 5 * time.Millisecond
		select {
		case p.sem <- struct{}{}:
		case <-p.ctx.Done():
			c.Close()
			return
		}
		if !p.track(c) {
			<-p.sem
			c.Close()
			return
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer func() { <-p.sem }()
			defer p.untrack(c)
			defer c.Close()
			p.handle(c)
		}()
	}
}

func reply(c net.Conn, code int, msg string) {
	c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	io.WriteString(c, "HTTP/1.1 "+strconv.Itoa(code)+" "+msg+"\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
}

func (p *Proxy) handle(c net.Conn) {
	c.SetReadDeadline(time.Now().Add(p.cfg.header))
	lr := &io.LimitedReader{R: c, N: maxHeader}
	br := bufio.NewReaderSize(lr, 8192)
	req, err := http.ReadRequest(br)
	if err != nil {
		if lr.N <= 0 {
			reply(c, 431, "Request Header Fields Too Large")
		} else {
			reply(c, 400, "Bad Request")
		}
		return
	}
	lr.N = math.MaxInt64
	c.SetReadDeadline(time.Time{})
	switch req.Method {
	case http.MethodConnect:
		p.connect(c, br, req)
	case http.MethodGet, http.MethodHead:
		p.forward(c, req)
	default:
		reply(c, 405, "Method Not Allowed")
	}
}

func (p *Proxy) dial(hostport string) (net.Conn, int) {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil || host == "" || port == "" {
		return nil, 400
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return nil, 400
	}
	if !p.cfg.allowPort(uint16(n)) {
		return nil, 403
	}
	ctx, cancel := context.WithTimeout(p.ctx, 30*time.Second)
	defer cancel()
	up, err := p.d.DialContext(ctx, "tcp", hostport)
	if err != nil {
		if errors.Is(err, ErrRefused) {
			return nil, 403
		}
		return nil, 502
	}
	if !p.track(up) {
		up.Close()
		return nil, 503
	}
	return up, 0
}

func (p *Proxy) connect(c net.Conn, br *bufio.Reader, req *http.Request) {
	up, code := p.dial(req.Host)
	if up == nil {
		reply(c, code, http.StatusText(code))
		return
	}
	defer p.untrack(up)
	defer up.Close()
	c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	io.WriteString(c, "HTTP/1.1 200 Connection established\r\n\r\n")
	c.SetWriteDeadline(time.Time{})

	// Idle means no bytes moved in either direction.
	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	done := make(chan struct{}, 2)
	pipe := func(dst io.Writer, src io.Reader) {
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				last.Store(time.Now().UnixNano())
				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}
				last.Store(time.Now().UnixNano())
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}
	stop := make(chan struct{})
	var wd sync.WaitGroup
	wd.Add(1)
	go func() {
		defer wd.Done()
		tick := p.cfg.idle / 4
		if tick < 5*time.Millisecond {
			tick = 5 * time.Millisecond
		}
		t := time.NewTicker(tick)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if time.Since(time.Unix(0, last.Load())) > p.cfg.idle {
					c.Close()
					up.Close()
					return
				}
			}
		}
	}()
	// br may hold bytes the client sent after the header (e.g. early TLS hello).
	go pipe(up, br)
	go pipe(c, up)
	<-done
	c.Close()
	up.Close()
	<-done
	close(stop)
	wd.Wait()
}

func stripHopByHop(h http.Header) {
	for _, v := range h.Values("Connection") {
		for _, tok := range strings.Split(v, ",") {
			if tok = strings.TrimSpace(tok); tok != "" {
				h.Del(tok)
			}
		}
	}
	for _, k := range []string{"Connection", "Proxy-Connection", "Proxy-Authorization",
		"Proxy-Authenticate", "Keep-Alive", "Te", "Trailer", "Upgrade", "Transfer-Encoding"} {
		h.Del(k)
	}
}

func (p *Proxy) forward(c net.Conn, req *http.Request) {
	if !req.URL.IsAbs() || req.URL.Scheme != "http" || req.URL.Host == "" ||
		req.ContentLength != 0 || len(req.TransferEncoding) > 0 {
		reply(c, 400, "Bad Request")
		return
	}
	host := req.URL.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		if strings.HasSuffix(host, ":") || strings.Contains(host, "]:") {
			reply(c, 400, "Bad Request")
			return
		}
		host = net.JoinHostPort(req.URL.Hostname(), "80")
	}
	up, code := p.dial(host)
	if up == nil {
		reply(c, code, http.StatusText(code))
		return
	}
	defer p.untrack(up)
	defer up.Close()
	out := req.Clone(p.ctx)
	out.RequestURI = ""
	out.Close = true
	stripHopByHop(out.Header)
	up.SetDeadline(time.Now().Add(p.cfg.idle))
	if err := out.Write(up); err != nil {
		reply(c, 502, "Bad Gateway")
		return
	}
	// Stream the response back verbatim until upstream closes (Connection: close).
	buf := make([]byte, 32*1024)
	for {
		up.SetReadDeadline(time.Now().Add(p.cfg.idle))
		n, err := up.Read(buf)
		if n > 0 {
			c.SetWriteDeadline(time.Now().Add(p.cfg.idle))
			if _, werr := c.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
