package egress

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type stubResolver struct {
	mu    sync.Mutex
	calls int
	seq   [][]netip.Addr
}

func (s *stubResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.calls
	s.calls++
	if i >= len(s.seq) {
		i = len(s.seq) - 1
	}
	return s.seq[i], nil
}

func localServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	}))
	t.Cleanup(srv.Close)
	_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
	return srv, port
}

func TestDialerRebinding(t *testing.T) {
	r := &stubResolver{seq: [][]netip.Addr{{netip.MustParseAddr("8.8.8.8")}, {netip.MustParseAddr("127.0.0.1")}}}
	var mu sync.Mutex
	var dialed []string
	d := &Dialer{Resolver: r, Inner: net.Dialer{Control: func(network, address string, _ syscall.RawConn) error {
		mu.Lock()
		dialed = append(dialed, address)
		mu.Unlock()
		return errors.New("stop")
	}}}
	d.AllowPorts = []uint16{8080}
	if _, err := d.DialContext(context.Background(), "tcp", "rebind.test:8080"); err == nil {
		t.Fatal("expected control error")
	}
	if r.calls != 1 {
		t.Fatalf("resolver called %d times, want 1", r.calls)
	}
	if len(dialed) != 1 || dialed[0] != "8.8.8.8:8080" {
		t.Fatalf("dialed %v, want the checked literal 8.8.8.8:8080", dialed)
	}
}

func TestDialerRedirectToPrivateRefused(t *testing.T) {
	_, port := localServer(t)
	// pub.test resolves to a public IP (filter passes; connection is faked to the
	// local server), then 302s to evil.test which resolves to loopback.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://evil.test:"+port+"/", http.StatusFound)
	}))
	defer srv.Close()
	r := &mapResolver{m: map[string]string{"pub.test": "8.8.8.8", "evil.test": "127.0.0.1"}}
	n, _ := strconv.Atoi(port)
	d := &Dialer{Resolver: r, AllowPorts: []uint16{80, uint16(n)}}
	cl := d.HTTPClient(2 * time.Second)
	tr := cl.Transport.(*http.Transport)
	real := tr.DialContext
	tr.DialContext = func(ctx context.Context, n, addr string) (net.Conn, error) {
		if strings.HasPrefix(addr, "pub.test:") {
			ips, _ := r.LookupNetIP(ctx, "ip", "pub.test")
			if Refused(ips[0]) {
				return nil, ErrRefused
			}
			return net.Dial("tcp", srv.Listener.Addr().String())
		}
		return real(ctx, n, addr)
	}
	_, err := cl.Get("http://pub.test:80/")
	if !errors.Is(err, ErrRefused) {
		t.Fatalf("err=%v, want ErrRefused on redirect hop", err)
	}
	if !strings.Contains(err.Error(), "evil.test") {
		t.Fatalf("hop 2 not exercised: %v", err)
	}
}

type mapResolver struct{ m map[string]string }

func (m *mapResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr(m.m[host])}, nil
}

func TestDialerAllowPrivate(t *testing.T) {
	_, port := localServer(t)
	r := &stubResolver{seq: [][]netip.Addr{{netip.MustParseAddr("127.0.0.1")}}}
	n, _ := strconv.Atoi(port)
	d := &Dialer{Resolver: r, AllowPrivate: true, AllowPorts: []uint16{uint16(n)}}
	resp, err := d.HTTPClient(2 * time.Second).Get("http://local.test:" + port + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.Status)
	}
}

func TestDialerLiteralAndNetwork(t *testing.T) {
	d := &Dialer{}
	if _, err := d.DialContext(context.Background(), "tcp", "127.0.0.1:1"); !errors.Is(err, ErrRefused) {
		t.Fatalf("literal: %v", err)
	}
	if _, err := d.DialContext(context.Background(), "udp", "8.8.8.8:53"); err == nil {
		t.Fatal("udp must error")
	}
}

func TestDialerRedirectSchemeAndLimit(t *testing.T) {
	d := &Dialer{AllowPrivate: true}
	c := d.HTTPClient(time.Second)
	req, _ := http.NewRequest("GET", "file:///etc/passwd", nil)
	if err := c.CheckRedirect(req, nil); err == nil {
		t.Fatal("file scheme redirect allowed")
	}
	req, _ = http.NewRequest("GET", "http://x.test/", nil)
	if err := c.CheckRedirect(req, make([]*http.Request, 10)); err == nil {
		t.Fatal("11th redirect allowed")
	}
	if err := c.CheckRedirect(req, make([]*http.Request, 9)); err != nil {
		t.Fatal(err)
	}
}

func TestDialerPortAllowlist(t *testing.T) {
	d := &Dialer{AllowPrivate: true}
	for _, p := range []uint16{80, 443} {
		if !d.PortAllowed(p) {
			t.Fatalf("default must allow %d", p)
		}
	}
	for _, p := range []uint16{22, 8080, 0} {
		if d.PortAllowed(p) {
			t.Fatalf("default must refuse %d", p)
		}
	}
	if _, err := d.DialContext(context.Background(), "tcp", "8.8.8.8:22"); !errors.Is(err, ErrRefused) {
		t.Fatalf("port 22: %v, want ErrRefused", err)
	}
	e := d.WithPorts(8080)
	if !e.PortAllowed(8080) || !e.PortAllowed(443) || d.PortAllowed(8080) {
		t.Fatal("WithPorts must add to a copy")
	}
}

// TestDialerRedirectToOtherPortRefused: the plain GET follows redirects, and
// each hop goes through the same port allowlist as the proxy.
func TestDialerRedirectToOtherPortRefused(t *testing.T) {
	var hits int
	var mu sync.Mutex
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
	}))
	defer target.Close()
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/", http.StatusFound)
	}))
	defer src.Close()
	d := &Dialer{AllowPrivate: true, AllowPorts: []uint16{portOf(t, src.URL)}}
	_, err := d.HTTPClient(2 * time.Second).Get(src.URL + "/")
	if !errors.Is(err, ErrRefused) {
		t.Fatalf("err=%v, want ErrRefused on the redirect to another port", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Fatal("the redirect target was reached")
	}
}

func TestHTTPClientHeaderTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)
	d := &Dialer{AllowPrivate: true, AllowPorts: []uint16{portOf(t, srv.URL)}}
	start := time.Now()
	_, err := d.httpClient(time.Minute, 100*time.Millisecond, time.Minute).Get(srv.URL)
	if err == nil || time.Since(start) > 30*time.Second {
		t.Fatalf("err=%v after %s, want a header timeout well before the overall timeout", err, time.Since(start))
	}
}

func TestHTTPClientBodyIdleTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("start"))
		w.(http.Flusher).Flush()
		<-release
	}))
	defer srv.Close()
	defer close(release)
	d := &Dialer{AllowPrivate: true, AllowPorts: []uint16{portOf(t, srv.URL)}}
	resp, err := d.httpClient(time.Minute, time.Minute, 150*time.Millisecond).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	start := time.Now()
	_, err = io.ReadAll(resp.Body)
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() || time.Since(start) > 30*time.Second {
		t.Fatalf("err=%v after %s, want an idle timeout on the stalled body", err, time.Since(start))
	}
}

func portOf(t *testing.T, rawURL string) uint16 {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	return uint16(p)
}
