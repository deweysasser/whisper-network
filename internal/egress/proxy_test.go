package egress

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
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
	p := startTestProxy(t, &Dialer{Resolver: r, AllowPrivate: true})
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
