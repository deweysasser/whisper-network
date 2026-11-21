package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/models"
)

var (
	both     = []api.TaskKind{api.KindPrepare, api.KindTranscribe}
	small    = models.Model{Name: "small", PeakRSS: 1 * gib}
	large    = models.Model{Name: "large", PeakRSS: 3 * gib}
	byMemory = []models.Model{small, large}
)

func TestMemBudgetOffersWhatFits(t *testing.T) {
	b := newMemBudget(4 * gib)
	ctx := context.Background()

	o1, r1, err := b.reserve(ctx, both, byMemory, 512*mib, 1*gib)
	require.NoError(t, err)
	assert.Equal(t, []string{"small", "large"}, o1.models)
	assert.Equal(t, both, o1.kinds)
	assert.Equal(t, 3*gib, b.inUse(), "provisionally reserves the largest offer")

	// The lease turned out to be a small model.
	r1.shrink(small.PeakRSS)
	assert.Equal(t, gib, b.inUse())

	o2, r2, err := b.reserve(ctx, both, byMemory, 512*mib, 1*gib)
	require.NoError(t, err)
	assert.Equal(t, []string{"small", "large"}, o2.models, "3 GiB still free")
	r2.shrink(large.PeakRSS)

	// 0 bytes free: a third slot waits until memory is returned.
	got := make(chan offer, 1)
	go func() {
		o, _, err := b.reserve(ctx, both, byMemory, 512*mib, 1*gib)
		if err == nil {
			got <- o
		}
	}()
	select {
	case <-got:
		t.Fatal("offered work with no free memory")
	case <-time.After(30 * time.Millisecond):
	}
	r1.release()
	select {
	case o := <-got:
		assert.Equal(t, []string{"small"}, o.models)
		assert.Equal(t, both, o.kinds)
	case <-time.After(5 * time.Second):
		t.Fatal("waiting slot was not woken")
	}
	r1.release() // idempotent
}

func TestMemBudgetPrepareOnlyAndCancel(t *testing.T) {
	b := newMemBudget(600 * mib)
	o, r, err := b.reserve(context.Background(), both, byMemory, 512*mib, 1*gib)
	require.NoError(t, err)
	assert.Equal(t, []api.TaskKind{api.KindPrepare}, o.kinds)
	assert.Empty(t, o.models)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _, err = b.reserve(ctx, both, byMemory, 512*mib, 1*gib)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	r.release()
	assert.Equal(t, int64(0), b.inUse())
}
