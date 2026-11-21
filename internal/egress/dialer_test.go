package egress

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
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
	d := &Dialer{Resolver: r}
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
	d := &Dialer{Resolver: r, AllowPrivate: true}
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
