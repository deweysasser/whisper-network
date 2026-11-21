package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/egress"
)

// proxies starts n egress proxies and returns them with a start function
// that hands them out in order.
func proxies(t *testing.T, n int) ([]*egress.Proxy, func() (*egress.Proxy, error)) {
	t.Helper()
	ps := make([]*egress.Proxy, n)
	for i := range ps {
		p, err := egress.StartProxy(context.Background(), &egress.Dialer{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = p.Close() })
		ps[i] = p
	}
	next := 0
	return ps, func() (*egress.Proxy, error) {
		p := ps[next]
		next++
		return p, nil
	}
}

func closed(p *egress.Proxy) bool {
	select {
	case <-p.Done():
		return true
	default:
		return false
	}
}

// TestStartProxyAvoidingRepicks: a proxy on a forbidden port is closed and
// the next one taken.
func TestStartProxyAvoidingRepicks(t *testing.T) {
	ps, start := proxies(t, 2)
	got, err := startProxyAvoiding(start, []uint16{80, ps[0].Port(), 443})
	require.NoError(t, err)
	assert.Same(t, ps[1], got)
	assert.True(t, closed(ps[0]), "the proxy on a forbidden port is closed")
	assert.False(t, closed(ps[1]))
}

// TestStartProxyAvoidingGivesUp: after proxyPortTries forbidden ports it
// fails, with every proxy it started closed.
func TestStartProxyAvoidingGivesUp(t *testing.T) {
	ps, start := proxies(t, proxyPortTries)
	var forbidden []uint16
	for _, p := range ps {
		forbidden = append(forbidden, p.Port())
	}
	_, err := startProxyAvoiding(start, forbidden)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no port outside")
	for i, p := range ps {
		assert.True(t, closed(p), "proxy %d left open", i)
	}
}
