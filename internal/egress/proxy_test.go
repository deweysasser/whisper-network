package egress

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"
)

func startTestProxy(t *testing.T, d *Dialer) *Proxy {
	t.Helper()
	p, err := StartProxy(t.Context(), d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func rawRequest(t *testing.T, p *Proxy, req string) (*bufio.Reader, net.Conn) {
	t.Helper()
	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", p.Port()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(3 * time.Second))
	io.WriteString(c, req)
	return bufio.NewReader(c), c
}

func status(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	l, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestProxyConnectRefusesPrivate(t *testing.T) {
	p := startTestProxy(t, &Dialer{})
	br, _ := rawRequest(t, p, "CONNECT 127.0.0.1:80 HTTP/1.1\r\nHost: 127.0.0.1:80\r\n\r\n")
	if l := status(t, br); !strings.Contains(l, " 403 ") {
		t.Fatal(l)
	}
}

func TestProxyConnectPortRestricted(t *testing.T) {
	p := startTestProxy(t, &Dialer{AllowPrivate: true})
	br, _ := rawRequest(t, p, "CONNECT 127.0.0.1:22 HTTP/1.1\r\nHost: 127.0.0.1:22\r\n\r\n")
	if l := status(t, br); !strings.Contains(l, " 403 ") {
		t.Fatal(l)
	}
}

func TestProxyMethodsAndForm(t *testing.T) {
	p := startTestProxy(t, &Dialer{AllowPrivate: true})
	br, _ := rawRequest(t, p, "POST http://x.test/ HTTP/1.1\r\nHost: x.test\r\n\r\n")
	if l := status(t, br); !strings.Contains(l, " 405 ") {
		t.Fatal(l)
	}
	br, _ = rawRequest(t, p, "GET /path HTTP/1.1\r\nHost: x.test\r\n\r\n")
	if l := status(t, br); !strings.Contains(l, " 400 ") {
		t.Fatal(l)
	}
}

func TestProxyForwardsAllowed(t *testing.T) {
	_, port := localServer(t)
	r := &stubResolver{seq: [][]netip.Addr{{netip.MustParseAddr("127.0.0.1")}}}
	p, err := startProxy(t.Context(), &Dialer{Resolver: r, AllowPrivate: true}, func(uint16) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d", p.Port())
	cl := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{
		Proxy: func(*http.Request) (*url.URL, error) { return url.Parse(proxyURL) },
	}}
	resp, err := cl.Get("http://fwd.test:" + port + "/x?y=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "hello" {
		t.Fatalf("body %q", b)
	}
}

func TestProxyForwardRefusedPrivate(t *testing.T) {
	_, port := localServer(t)
	p := startTestProxy(t, &Dialer{})
	br, _ := rawRequest(t, p, "GET http://127.0.0.1:"+port+"/ HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
	if l := status(t, br); !strings.Contains(l, " 403 ") {
		t.Fatal(l)
	}
}

func TestProxyConnectTunnels(t *testing.T) {
	// Echo server on a port we can't use (80/443 restricted), so test tunnel via
	// the internal port allow-list override.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		io.Copy(c, c)
		c.Close()
	}()
	p, err := startProxy(t.Context(), &Dialer{AllowPrivate: true}, func(uint16) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	addr := ln.Addr().String()
	br, c := rawRequest(t, p, "CONNECT "+addr+" HTTP/1.1\r\nHost: "+addr+"\r\n\r\n")
	if l := status(t, br); !strings.Contains(l, " 200 ") {
		t.Fatal(l)
	}
	br.ReadString('\n')
	io.WriteString(c, "ping")
	buf := make([]byte, 4)
	if _, err := io.ReadFull(br, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("%q %v", buf, err)
	}
}

func startCfg(t *testing.T, d *Dialer, cfg proxyConfig) *Proxy {
	t.Helper()
	p, err := startProxyCfg(t.Context(), d, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func TestProxyForwardPortRestricted(t *testing.T) {
	_, port := localServer(t)
	p := startTestProxy(t, &Dialer{AllowPrivate: true})
	br, _ := rawRequest(t, p, "GET http://127.0.0.1:"+port+"/ HTTP/1.1\r\nHost: x\r\n\r\n")
	if l := status(t, br); !strings.Contains(l, " 403 ") {
		t.Fatal(l)
	}
}

func TestProxyEmptyPort400(t *testing.T) {
	p := startTestProxy(t, &Dialer{AllowPrivate: true})
	br, _ := rawRequest(t, p, "CONNECT x.test: HTTP/1.1\r\nHost: x.test:\r\n\r\n")
	if l := status(t, br); !strings.Contains(l, " 400 ") {
		t.Fatal(l)
	}
}

func TestProxyHeaderCap(t *testing.T) {
	p := startTestProxy(t, &Dialer{AllowPrivate: true})
	br, _ := rawRequest(t, p, "GET http://x.test/ HTTP/1.1\r\nX: "+strings.Repeat("a", 200<<10)+"\r\n\r\n")
	if l := status(t, br); !strings.Contains(l, " 431 ") {
		t.Fatal(l)
	}
}

func TestProxyHeaderTimeout(t *testing.T) {
	p := startCfg(t, &Dialer{AllowPrivate: true}, proxyConfig{header: 150 * time.Millisecond})
	br, _ := rawRequest(t, p, "GET http://x.test/ HTTP/1.1\r\n")
	start := time.Now()
	if l := status(t, br); !strings.Contains(l, " 400 ") {
		t.Fatal(l)
	}
	if time.Since(start) > time.Second {
		t.Fatal("header timeout too slow")
	}
}

func TestProxyForwardBodyRejected(t *testing.T) {
	p := startCfg(t, &Dialer{AllowPrivate: true}, proxyConfig{allowPort: func(uint16) bool { return true }})
	br, _ := rawRequest(t, p, "GET http://127.0.0.1:1/ HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello")
	if l := status(t, br); !strings.Contains(l, " 400 ") {
		t.Fatal(l)
	}
}

func TestProxyForwardStripsHeaders(t *testing.T) {
	got := make(chan http.Header, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Clone()
	}))
	defer srv.Close()
	p := startCfg(t, &Dialer{AllowPrivate: true}, proxyConfig{allowPort: func(uint16) bool { return true }})
	br, _ := rawRequest(t, p, "GET http://"+srv.Listener.Addr().String()+"/ HTTP/1.1\r\nHost: x\r\n"+
		"Proxy-Authorization: Basic secret\r\nConnection: X-Secret, keep-alive\r\nX-Secret: s\r\n"+
		"Keep-Alive: 5\r\nUpgrade: websocket\r\nTE: trailers\r\nX-Keep: ok\r\n\r\n")
	status(t, br)
	h := <-got
	for _, k := range []string{"Proxy-Authorization", "X-Secret", "Keep-Alive", "Upgrade", "Te"} {
		if h.Get(k) != "" {
			t.Errorf("header %s leaked: %q", k, h.Get(k))
		}
	}
	if h.Get("X-Keep") != "ok" {
		t.Error("end-to-end header dropped")
	}
}

func TestProxyConnectIdleTracksBothDirections(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 8)
		c.Read(buf)
		for i := 0; i < 30; i++ {
			c.Write([]byte("x"))
			time.Sleep(100 * time.Millisecond)
		}
	}()
	p := startCfg(t, &Dialer{AllowPrivate: true}, proxyConfig{allowPort: func(uint16) bool { return true }, idle: time.Second})
	addr := ln.Addr().String()
	br, c := rawRequest(t, p, "CONNECT "+addr+" HTTP/1.1\r\nHost: "+addr+"\r\n\r\n")
	c.SetDeadline(time.Now().Add(10 * time.Second))
	status(t, br)
	br.ReadString('\n')
	io.WriteString(c, "req")
	n, _ := io.Copy(io.Discard, br)
	if n != 30 {
		t.Fatalf("got %d bytes, want 30 (one-way download killed by idle timer)", n)
	}
}

func TestProxyConnectIdleExpires(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			defer c.Close()
			io.Copy(io.Discard, c)
		}
	}()
	p := startCfg(t, &Dialer{AllowPrivate: true}, proxyConfig{allowPort: func(uint16) bool { return true }, idle: 300 * time.Millisecond})
	addr := ln.Addr().String()
	br, c := rawRequest(t, p, "CONNECT "+addr+" HTTP/1.1\r\nHost: "+addr+"\r\n\r\n")
	status(t, br)
	br.ReadString('\n')
	start := time.Now()
	if _, err := br.ReadByte(); err == nil {
		t.Fatal("expected close")
	}
	_ = c
	if time.Since(start) > 2*time.Second {
		t.Fatal("idle close too slow")
	}
}

func TestProxyConnCap(t *testing.T) {
	p := startCfg(t, &Dialer{AllowPrivate: true}, proxyConfig{maxConns: 1})
	hold, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", p.Port()))
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Close()
	time.Sleep(50 * time.Millisecond)
	br, c := rawRequest(t, p, "CONNECT 127.0.0.1:22 HTTP/1.1\r\nHost: x\r\n\r\n")
	c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, err := br.ReadString('\n'); err == nil {
		t.Fatal("second conn served despite cap")
	}
}
