package egress

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Proxy is a loopback HTTP proxy that dials only through a filtering Dialer.
type Proxy struct {
	ln        net.Listener
	d         *Dialer
	allowPort func(uint16) bool
	wg        sync.WaitGroup
	mu        sync.Mutex
	conns     map[net.Conn]struct{}
	closed    bool
}

const (
	headerTimeout = 15 * time.Second
	idleTimeout   = 2 * time.Minute
)

func defaultAllowPort(p uint16) bool { return p == 80 || p == 443 }

// StartProxy listens on 127.0.0.1:0 and serves until ctx is done or Close.
func StartProxy(ctx context.Context, d *Dialer) (*Proxy, error) {
	return startProxy(ctx, d, defaultAllowPort)
}

func startProxy(ctx context.Context, d *Dialer, allow func(uint16) bool) (*Proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{ln: ln, d: d, allowPort: allow, conns: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go p.serve()
	go func() { <-ctx.Done(); p.Close() }()
	return p, nil
}

// Port is the listening TCP port.
func (p *Proxy) Port() uint16 { return uint16(p.ln.Addr().(*net.TCPAddr).Port) }

// Close stops the listener, drops live connections and waits for handlers.
func (p *Proxy) Close() error {
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

func (p *Proxy) serve() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		if !p.track(c) {
			c.Close()
			return
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer p.untrack(c)
			defer c.Close()
			p.handle(c)
		}()
	}
}

func reply(c net.Conn, code int, msg string) {
	io.WriteString(c, "HTTP/1.1 "+strconv.Itoa(code)+" "+msg+"\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
}

func (p *Proxy) handle(c net.Conn) {
	c.SetReadDeadline(time.Now().Add(headerTimeout))
	br := bufio.NewReaderSize(c, 8192)
	req, err := http.ReadRequest(br)
	if err != nil {
		reply(c, 400, "Bad Request")
		return
	}
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

func (p *Proxy) dial(ctx context.Context, hostport string, restrictPort bool) (net.Conn, int) {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil || host == "" {
		return nil, 400
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || (restrictPort && !p.allowPort(uint16(n))) {
		return nil, 403
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	up, err := p.d.DialContext(ctx, "tcp", hostport)
	if err != nil {
		if errors.Is(err, ErrRefused) {
			return nil, 403
		}
		return nil, 502
	}
	return up, 0
}

func (p *Proxy) connect(c net.Conn, br *bufio.Reader, req *http.Request) {
	up, code := p.dial(context.Background(), req.Host, true)
	if up == nil {
		reply(c, code, http.StatusText(code))
		return
	}
	defer up.Close()
	io.WriteString(c, "HTTP/1.1 200 Connection established\r\n\r\n")
	done := make(chan struct{}, 2)
	pipe := func(dst net.Conn, src io.Reader) {
		buf := make([]byte, 32*1024)
		for {
			dst.SetDeadline(time.Now().Add(idleTimeout))
			n, err := src.Read(buf)
			if n > 0 {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}
	// br may hold bytes the client sent after the header (e.g. early TLS hello).
	go pipe(up, br)
	go pipe(c, up)
	<-done
	// Either side finished: close both so the other copier unblocks.
	c.Close()
	up.Close()
	<-done
}

func (p *Proxy) forward(c net.Conn, req *http.Request) {
	if !req.URL.IsAbs() || req.URL.Scheme != "http" || req.URL.Host == "" {
		reply(c, 400, "Bad Request")
		return
	}
	host := req.URL.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(req.URL.Hostname(), "80")
	}
	up, code := p.dial(context.Background(), host, false)
	if up == nil {
		reply(c, code, http.StatusText(code))
		return
	}
	defer up.Close()
	out := req.Clone(context.Background())
	out.RequestURI = ""
	out.Close = true
	out.Header.Del("Proxy-Connection")
	out.Header.Del("Proxy-Authorization")
	up.SetDeadline(time.Now().Add(idleTimeout))
	if err := out.Write(up); err != nil {
		reply(c, 502, "Bad Gateway")
		return
	}
	// Stream the response back verbatim until upstream closes (Connection: close).
	buf := make([]byte, 32*1024)
	for {
		up.SetReadDeadline(time.Now().Add(idleTimeout))
		n, err := up.Read(buf)
		if n > 0 {
			c.SetWriteDeadline(time.Now().Add(idleTimeout))
			if _, werr := c.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
