package server

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/obs"
)

func segs(texts ...string) []api.Segment {
	out := make([]api.Segment, len(texts))
	for i, t := range texts {
		out[i] = api.Segment{StartMS: int64(i) * 10, EndMS: int64(i)*10 + 10, Text: t}
	}
	return out
}

func repeat(text string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = text
	}
	return out
}

func varied(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("line %d", i)
	}
	return out
}

func TestDetectLoop(t *testing.T) {
	loopLine := " Please stand for a moment of silence."
	tests := []struct {
		name  string
		texts []string
		loop  bool
	}{
		{"empty", nil, false},
		{"varied speech", varied(500), false},
		{"run of 51 in a row", append(varied(400), repeat(loopLine, 51)...), true},
		{"run of 50 in a row is allowed", append(varied(400), repeat(loopLine, 50)...), false},
		{"run matches despite case, spacing and end punctuation", append(varied(400),
			append(repeat("please stand for a moment of silence", 30), repeat("  Please  stand for a moment of silence!", 21)...)...), true},
		// 40 scattered repeats among 60 lines: 40% of segments but only 40
		// occurrences, too few to call a loop (a short task saying
		// "Thank you." a lot).
		{"few repeats in a short task", interleave(repeat("Thank you.", 40), varied(20)), false},
		// 60 scattered repeats, never 51 in a row, among 100 lines: over 30%.
		{"scattered repeats over 30%", interleave(repeat(loopLine, 60), varied(40)), true},
		{"scattered repeats under 30%", interleave(repeat(loopLine, 60), varied(200)), false},
		{"blank segments are ignored", append(varied(10), repeat("   ", 300)...), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := detectLoop(segs(tc.texts...))
			if tc.loop {
				require.NotNil(t, got)
				assert.Contains(t, got.Error(), "repetition loop")
				assert.NotContains(t, got.Error(), "silence", "the reason never quotes transcript text")
			} else {
				assert.Nil(t, got)
			}
		})
	}
}

// interleave spreads a among b so that no two elements of a are adjacent
// while b lasts.
func interleave(a, b []string) []string {
	var out []string
	for len(a) > 0 || len(b) > 0 {
		if len(a) > 0 {
			out, a = append(out, a[0]), a[1:]
		}
		if len(b) > 0 {
			out, b = append(out, b[0]), b[1:]
		}
	}
	return out
}

func loopResult(l *api.Lease) api.TranscribeResult {
	res := result(l, "")
	res.Segments = segs(repeat(" Please stand for a moment of silence.", 200)...)
	for i := range res.Segments { // fit the 1 s task
		res.Segments[i].StartMS, res.Segments[i].EndMS = int64(i)*4, int64(i)*4+4
	}
	return res
}

// TestLoopedResultRetriesWithoutContext: a transcript stuck repeating one
// line is refused, not stored. The task goes back to pending without
// counting toward the worker's breaker; the retry, on another worker,
// carries no_context and no prompt; a second loop fails the job (#99).
func TestLoopedResultRetriesWithoutContext(t *testing.T) {
	t.Parallel()
	h := newHarness(t, failureLimit(1))
	client := h.client(submitOn("public"))
	aID, aTok := h.activeWorker(api.WorkerFacts{}, "public")
	bID, bTok := h.activeWorker(api.WorkerFacts{}, "public")
	job := h.submit(client, "public", api.JobParams{Model: "small.en", Prompt: "Councillor Smith"},
		canonicalWAV(speech(1, media.SampleRate)))

	la := h.claim(aID, aTok, "", nil)
	require.NotNil(t, la)
	assert.False(t, la.NoContext)
	assert.Equal(t, "Councillor Smith", la.Prompt)

	resp, body := h.putResult(la, aTok, loopResult(la))
	h.expectProblem(resp, body, http.StatusUnprocessableEntity, api.ProblemResultInvalid)

	failed := h.logs.events(t, obs.EventTaskFailed)
	require.Len(t, failed, 1)
	assert.Equal(t, "loop", failed[0]["stage"])
	assert.Empty(t, h.logs.events(t, obs.EventWorkerAutoPaused), "a loop is not the worker's fault")
	j := h.job(job.ID)
	assert.Equal(t, api.JobRunning, j.State)
	assert.Equal(t, 1, j.Tasks.Pending)

	lb := h.claim(bID, bTok, "", nil)
	require.NotNil(t, lb)
	assert.True(t, lb.NoContext)
	assert.Empty(t, lb.Prompt, "the retry drops the prompt")

	resp, body = h.putResult(lb, bTok, loopResult(lb))
	h.expectProblem(resp, body, http.StatusUnprocessableEntity, api.ProblemResultInvalid)
	j = h.job(job.ID)
	assert.Equal(t, api.JobFailed, j.State)
	require.NotNil(t, j.Error)
	assert.Contains(t, *j.Error, "repetition loop")
	// Transcript text needs read permission; the job error and the logs
	// are visible without it (DESIGN.md §10.1).
	assert.NotContains(t, *j.Error, "silence")
	assert.NotContains(t, string(h.logs.Bytes()), "silence")
}

// TestLoopRetryCleanResult: a clean result on the no-context retry
// completes the job.
func TestLoopRetryCleanResult(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	client := h.client(submitOn("public"))
	aID, aTok := h.activeWorker(api.WorkerFacts{}, "public")
	bID, bTok := h.activeWorker(api.WorkerFacts{}, "public")
	job := h.submit(client, "public", api.JobParams{Model: "small.en"}, canonicalWAV(speech(1, media.SampleRate)))

	la := h.claim(aID, aTok, "", nil)
	resp, body := h.putResult(la, aTok, loopResult(la))
	h.expectProblem(resp, body, http.StatusUnprocessableEntity, api.ProblemResultInvalid)

	lb := h.claim(bID, bTok, "", nil)
	require.NotNil(t, lb)
	resp, body = h.putResult(lb, bTok, result(lb, "clean"))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	assert.Equal(t, api.JobDone, h.job(job.ID).State)
}

// TestLoopsDoNotTripBreaker: loops on two jobs in a row, which as ordinary
// failures would pause the worker at a limit of 2, leave it active.
func TestLoopsDoNotTripBreaker(t *testing.T) {
	t.Parallel()
	h := newHarness(t, failureLimit(2))
	client := h.client(submitOn("public"))
	wid, wtok := h.activeWorker(api.WorkerFacts{}, "public")
	for range 2 {
		h.submit(client, "public", api.JobParams{Model: "small.en"}, canonicalWAV(speech(1, media.SampleRate)))
	}
	for i := range 2 {
		l := h.claim(wid, wtok, "", nil)
		require.NotNil(t, l, "claim %d", i)
		resp, body := h.putResult(l, wtok, loopResult(l))
		h.expectProblem(resp, body, http.StatusUnprocessableEntity, api.ProblemResultInvalid)
	}
	assert.Empty(t, h.logs.events(t, obs.EventWorkerAutoPaused))
}
