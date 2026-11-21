package egress

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
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
	_, port := localServer(t)
	// First lookup is "public" (8.8.8.8, unroutable here -> would hang/fail), second private.
	r := &stubResolver{seq: [][]netip.Addr{{netip.MustParseAddr("8.8.8.8")}, {netip.MustParseAddr("127.0.0.1")}}}
	d := &Dialer{Resolver: r, Inner: net.Dialer{Timeout: 200 * time.Millisecond}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := d.DialContext(ctx, "tcp", "rebind.test:"+port)
	if err == nil {
		// If we reached 127.0.0.1 we dialed an unchecked address.
		c.Close()
		t.Fatal("connected to private address: second resolution used")
	}
	if r.calls != 1 {
		t.Fatalf("resolver called %d times, want 1", r.calls)
	}
}

func TestDialerRedirectToPrivateRefused(t *testing.T) {
	_, port := localServer(t)
	r := &stubResolver{seq: [][]netip.Addr{{netip.MustParseAddr("127.0.0.1")}}}
	d := &Dialer{Resolver: r}
	_, err := d.HTTPClient(2 * time.Second).Get("http://evil.test:" + port + "/")
	if !errors.Is(err, ErrRefused) {
		t.Fatalf("err=%v, want ErrRefused", err)
	}
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
