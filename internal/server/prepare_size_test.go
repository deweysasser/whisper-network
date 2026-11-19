package server

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

// TestPrepareSizeRouting: a claim whose gauges say how large a prepare
// input the worker can take gets no prepare task with a larger original;
// prepare leases and the worker document carry the server's bound on the
// canonical result, so the worker can size its disk admission by it.
func TestPrepareSizeRouting(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitOn("public"))
	wid, wtok := h.activeWorker(api.WorkerFacts{SandboxLevel: api.SandboxFull}, "public")
	orig := []byte("some video bytes, 32 of them....")
	h.submit(client, "public", api.JobParams{Model: "small.en"}, orig)

	var doc api.Worker
	h.expect("GET", "/worker/"+wid, wtok, nil, http.StatusOK, &doc)
	assert.Equal(t, h.srv.maxCanonicalBytes(), doc.MaxPrepareOutputBytes)

	claim := func(maxInput int64) *api.Lease {
		g := &api.Gauges{PrepareMaxInputBytes: &maxInput}
		return h.claim(wid, wtok, "", &api.ClaimRequest{Kinds: []api.TaskKind{api.KindPrepare},
			Models: []string{"small.en"}, Gauges: g})
	}
	assert.Nil(t, claim(int64(len(orig))-1), "the original is larger than the worker can take")
	assert.Nil(t, claim(-5), "a negative bound offers nothing")
	l := claim(int64(len(orig)))
	require.NotNil(t, l, "it fits exactly")
	assert.Equal(t, api.KindPrepare, l.Kind)
	assert.Equal(t, h.srv.maxCanonicalBytes(), l.MaxOutputBytes)
}
