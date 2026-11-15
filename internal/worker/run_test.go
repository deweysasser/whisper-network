package worker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/obs/obstest"
	"github.com/deweysasser/whisper-network/internal/worker/workertest"
)

const secretPrompt = "Councilors Zebediah Quixote and Philomena Snodgrass discuss the budget"

func TestTranscribeEndToEnd(t *testing.T) {
	h := newHarness(t)
	h.cfg.ChildEnv = []string{workertest.WhisperMode(workertest.SleepMode("150ms"))}
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(12), Model: "tiny.test",
		Prompt: secretPrompt, VADModel: "vad.test"})
	cancel, done := h.run()
	h.activate()
	h.eventually(h.taskState(task, api.TaskDone), "transcribe task never finished")
	// The server records the result before the worker hears back. Stopping
	// the worker in between would abandon the upload and release the lease,
	// so wait for the worker's own record of the task first.
	h.waitEvent("task.done")
	cancel()
	require.NoError(t, waitDone(t, done))

	res := h.srv.Task(task).Transcript
	require.NotNil(t, res)
	assert.Equal(t, "whisper.cpp "+workertest.FakeWhisperVersion, res.EngineVersion)
	assert.Equal(t, "tiny.test", res.Model)
	assert.Equal(t, testCatalog()[0].SHA256, res.ModelSHA256)
	assert.Equal(t, "vad.test", res.VAD)
	assert.Equal(t, "en", res.Language)
	assert.GreaterOrEqual(t, res.ComputeMS, int64(150))
	assert.Equal(t, []api.Segment{
		{StartMS: 0, EndMS: 5000, Text: "segment 0"},
		{StartMS: 5000, EndMS: 10000, Text: "segment 1"},
		{StartMS: 10000, EndMS: 12000, Text: "segment 2"},
	}, res.Segments)

	leases := h.srv.LeasesFor(task)
	require.Len(t, leases, 1)
	assert.Equal(t, api.OutcomeDone, leases[0].Outcome)
	assert.Positive(t, leases[0].Heartbeats, "a 150 ms run spans several scaled 30 s heartbeats")
	require.NotEmpty(t, leases[0].Gauges)
	g := leases[0].Gauges[0]
	assert.Equal(t, 1, g.SlotsBusy)
	assert.Equal(t, 0.5, g.Load1)
	assert.Equal(t, 2.5, g.CPUThrottledSecs)

	assert.Equal(t, 1, h.srv.ModelDownloads("tiny.test"))
	assert.Equal(t, 1, h.srv.ModelDownloads("vad.test"))
	entries, _ := os.ReadDir(filepath.Join(h.cfg.StateDir, "work"))
	assert.Empty(t, entries, "the lease work directory is removed")

	logs := h.logs.Bytes()
	obstest.AssertClean(t, logs)
	assert.NotContains(t, string(logs), "Zebediah", "prompt text must never be logged")
	assert.NotContains(t, string(logs), "segment 1", "transcript text must never be logged")
	done1 := findEvent(t, logs, "task.done")
	assert.Equal(t, leases[0].ID, done1["lease_id"])
	assert.Contains(t, done1, "compute_ms")
	assert.Contains(t, done1, "rtf")
	assert.Equal(t, 12.0, done1["audio_s"])
	assert.NotEmpty(t, done1["worker_id"])
}

func TestPrepareEndToEnd(t *testing.T) {
	h := newHarness(t)
	pcm := bytes.Repeat([]byte{1, 0, 2, 0, 3, 0, 4, 0}, 16000)
	task := h.srv.AddTask(workertest.Task{Kind: api.KindPrepare, Input: pcm, SendInputHash: true})
	cancel, done := h.run()
	h.activate()
	h.eventually(h.taskState(task, api.TaskDone), "prepare task never finished")
	h.waitEvent("task.done")
	cancel()
	require.NoError(t, waitDone(t, done))

	got := h.srv.Task(task)
	info, err := media.ValidateFLAC(bytes.NewReader(got.FLAC), 0)
	require.NoError(t, err)
	assert.Equal(t, int64(len(pcm)/2), info.Samples)
	assert.Equal(t, workertest.FakeFFmpegVersion, got.FFmpegVersion)
	assert.NotEmpty(t, got.ComputeMS)
	obstest.AssertClean(t, h.logs.Bytes())
}

func TestInputDownloadResumes(t *testing.T) {
	h := newHarness(t)
	pcm := bytes.Repeat([]byte("0123456789abcdef"), 4000)
	prep := h.srv.AddTask(workertest.Task{Kind: api.KindPrepare, Input: pcm, SendInputHash: true})
	h.srv.CutInputs(2, 20000)
	_, _ = h.run()
	h.activate()
	h.eventually(h.taskState(prep, api.TaskDone), "prepare never finished")
	l := h.srv.LeasesFor(prep)
	require.Len(t, l, 1, "a transient cut is resumed within the same lease")
	assert.Equal(t, []string{"", "bytes=20000-", "bytes=40000-"}, l[0].Inputs)

	// Transcribe input is regenerated: the server ignores Range and the
	// worker starts over.
	tr := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(3), Model: "tiny.test"})
	h.srv.CutInputs(1, 5000)
	h.eventually(h.taskState(tr, api.TaskDone), "transcribe never finished")
	l = h.srv.LeasesFor(tr)
	require.Len(t, l, 1)
	assert.Equal(t, []string{"", "bytes=5000-"}, l[0].Inputs)
	assert.Len(t, h.srv.Task(tr).Transcript.Segments, 1)
}

func TestResultUploadRetriesTransientErrors(t *testing.T) {
	h := newHarness(t)
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test"})
	h.srv.FailResults(3, http.StatusServiceUnavailable)
	_, _ = h.run()
	h.activate()
	h.eventually(h.taskState(task, api.TaskDone), "never finished")
	assert.Equal(t, 4, h.srv.ResultUploads())
	assert.Len(t, h.srv.LeasesFor(task), 1)
	assert.Contains(t, h.sleeps.all(), 4*time.Second, "backoff 1s, 2s, 4s between attempts")
}

func TestResultUploadStopsOnGone(t *testing.T) {
	h := newHarness(t)
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test"})
	h.srv.FailResults(100, http.StatusGone)
	_, _ = h.run()
	h.activate()
	h.eventually(func() bool { return h.srv.ResultUploads() >= 1 }, "no upload")
	// The worker moves on to claim again without retrying or reporting.
	n := len(h.srv.Claims())
	h.eventually(func() bool { return len(h.srv.Claims()) > n }, "worker did not move on")
	assert.Equal(t, 1, h.srv.ResultUploads())
	assert.Empty(t, h.srv.Task(task).Failures)
	assert.Contains(t, string(h.logs.Bytes()), `"outcome":"cancelled"`)
}

// TestHeartbeatGoneWhileResultAcceptedIsDone: the server accepted the
// result (ending the lease) but its answer is slow, and heartbeats in the
// meantime get 410. The upload's answer decides: the task is done, not
// abandoned. Before the fix the 410 cancelled the in-flight upload.
func TestHeartbeatGoneWhileResultAcceptedIsDone(t *testing.T) {
	h := newHarness(t)
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test"})
	h.srv.SlowResultAcks(300 * time.Millisecond) // ten scaled heartbeat intervals
	_, _ = h.run()
	h.activate()
	h.eventually(h.taskState(task, api.TaskDone), "never finished")
	h.eventually(func() bool { return bytes.Contains(h.logs.Bytes(), []byte(`"event":"task.done"`)) },
		"the worker never recorded the task as done")
	assert.NotContains(t, string(h.logs.Bytes()), `"outcome":"cancelled"`)
	assert.Equal(t, 1, h.srv.ResultUploads())
}

func TestResultRejectedIsNotReportedAgain(t *testing.T) {
	h := newHarness(t)
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test"})
	h.srv.FailResults(1, http.StatusUnprocessableEntity)
	_, _ = h.run()
	h.activate()
	h.eventually(func() bool { return h.srv.ResultUploads() >= 1 }, "no upload")
	n := len(h.srv.Claims())
	h.eventually(func() bool { return len(h.srv.Claims()) > n }, "worker did not move on")
	assert.Equal(t, 1, h.srv.ResultUploads(), "a 422 is not retried")
	assert.Empty(t, h.srv.Task(task).Failures, "the 422 itself counts as the failure; no extra report")
	ev := findEvent(t, h.logs.Bytes(), "task.failed")
	assert.Equal(t, "failed", ev["outcome"])
}

func TestWorkerClampsSegmentsToTheTask(t *testing.T) {
	h := newHarness(t)
	// The WAV header claims 4 s but the data holds 1 s; the fake whisper
	// trusts the header. The worker clamps to the lease's audio length so
	// the server's range check passes.
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(4)[:44+16000*2], Model: "tiny.test"})
	_, _ = h.run()
	h.activate()
	h.eventually(h.taskState(task, api.TaskDone), "never finished")
	assert.Equal(t, []api.Segment{{StartMS: 0, EndMS: 1000, Text: "segment 0"}}, h.srv.Task(task).Transcript.Segments)
}

func TestHeartbeat410KillsChildPromptly(t *testing.T) {
	h := newHarness(t)
	h.cfg.ChildEnv = []string{workertest.WhisperMode(workertest.ModeHang)}
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test"})
	_, _ = h.run()
	h.activate()
	l := h.liveLease(task)
	h.eventually(func() bool { return h.srv.LeasesFor(task)[0].Heartbeats >= 2 }, "no heartbeats")
	claimsBefore := len(h.srv.Claims())

	start := time.Now()
	h.srv.CancelLease(l.ID)
	// The slot can only claim again once the hung child has exited.
	h.eventually(func() bool { return len(h.srv.Claims()) > claimsBefore }, "child was not killed")
	assert.Less(t, time.Since(start), 5*time.Second)
	got := h.srv.Task(task)
	assert.Empty(t, got.Failures, "a cancelled lease is not reported as failed")
	assert.Equal(t, 0, h.srv.ResultUploads())
}

func TestShutdownReleasesLeases(t *testing.T) {
	h := newHarness(t)
	h.cfg.ChildEnv = []string{workertest.WhisperMode(workertest.ModeHang)}
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test"})
	cancel, done := h.run()
	h.activate()
	h.liveLease(task)
	// Wait until the worker itself holds the lease (it heartbeats only for
	// leases it received). Cancelling while the claim response is still in
	// flight tests a different case: the worker never learns of the lease.
	h.eventually(func() bool { return h.srv.LeasesFor(task)[0].Heartbeats >= 1 }, "worker never held the lease")
	cancel()
	require.NoError(t, waitDone(t, done))
	l := h.srv.LeasesFor(task)
	require.Len(t, l, 1)
	assert.Equal(t, api.OutcomeReleased, l[0].Outcome)
	got := h.srv.Task(task)
	assert.Equal(t, api.TaskPending, got.State, "released task requeues at once")
	assert.Equal(t, 0, got.Attempts, "a release does not consume an attempt")
	assert.Contains(t, string(h.logs.Bytes()), `"event":"lease.released"`)
}

func TestDrainFinishesRunningTasks(t *testing.T) {
	h := newHarness(t)
	h.cfg.Drain = true
	h.cfg.ChildEnv = []string{workertest.WhisperMode(workertest.SleepMode("400ms"))}
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test"})
	other := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test"})
	cancel, done := h.run()
	h.activate()
	h.liveLease(task)
	// Wait until the worker itself holds the lease (it heartbeats only for
	// leases it received). Cancelling while the claim response is still in
	// flight tests a different case: the worker never learns of the lease.
	h.eventually(func() bool { return h.srv.LeasesFor(task)[0].Heartbeats >= 1 }, "worker never held the lease")
	cancel()
	require.NoError(t, waitDone(t, done))
	l := h.srv.LeasesFor(task)
	require.Len(t, l, 1)
	assert.Equal(t, api.OutcomeDone, l[0].Outcome)
	assert.Equal(t, api.TaskDone, h.srv.Task(task).State)
	assert.Empty(t, h.srv.LeasesFor(other), "no claims after the shutdown signal")
}

// TestDrainAbortReleasesRunningTasks: while draining, closing Abort (the
// command's second signal) kills the running child and releases its lease
// without consuming an attempt, then the worker exits.
func TestDrainAbortReleasesRunningTasks(t *testing.T) {
	h := newHarness(t)
	abort := make(chan struct{})
	h.cfg.Drain = true
	h.cfg.Abort = abort
	h.cfg.ChildEnv = []string{workertest.WhisperMode(workertest.ModeHang)}
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test"})
	cancel, done := h.run()
	h.activate()
	h.liveLease(task)
	h.eventually(func() bool { return h.srv.LeasesFor(task)[0].Heartbeats >= 1 }, "worker never held the lease")

	cancel() // first signal: drain
	h.eventually(func() bool { return strings.Contains(string(h.logs.Bytes()), `"event":"worker.draining"`) },
		"no draining event")
	select {
	case err := <-done:
		t.Fatalf("worker exited during the drain of a hung task: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	start := time.Now()
	close(abort) // second signal: abort the drain
	require.NoError(t, waitDone(t, done))
	assert.Less(t, time.Since(start), 5*time.Second, "abort is prompt")
	l := h.srv.LeasesFor(task)
	require.Len(t, l, 1)
	assert.Equal(t, api.OutcomeReleased, l[0].Outcome)
	got := h.srv.Task(task)
	assert.Equal(t, api.TaskPending, got.State)
	assert.Equal(t, 0, got.Attempts, "an aborted drain does not consume an attempt")
	assert.Empty(t, got.Failures, "an aborted drain is not a failure")
}

// TestDrainWhileIdleExitsPromptly: with nothing running, a drain ends at
// once.
func TestDrainWhileIdleExitsPromptly(t *testing.T) {
	h := newHarness(t)
	h.cfg.Drain = true
	cancel, done := h.run()
	h.activate()
	h.eventually(func() bool { return len(h.srv.Claims()) > 0 }, "worker never polled")
	start := time.Now()
	cancel()
	require.NoError(t, waitDone(t, done))
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestChildFailureIsReportedWithStderr(t *testing.T) {
	h := newHarness(t)
	h.cfg.ChildEnv = []string{workertest.WhisperMode(workertest.ModeCrash)}
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(2), Model: "tiny.test",
		Prompt: secretPrompt})
	_, _ = h.run()
	h.activate()
	h.eventually(h.taskState(task, api.TaskFailed), "task did not fail")
	f := h.srv.Task(task).Failures
	require.Len(t, f, 2, "retryable failure is retried up to max attempts")
	assert.True(t, f[0].Retryable)
	assert.Contains(t, f[0].Reason, "whisper-cli failed (exit status 3)")
	assert.Equal(t, api.StageChild, f[0].Stage)
	assert.Contains(t, f[0].StderrTail, "simulated crash")
	logs := h.logs.Bytes()
	obstest.AssertClean(t, logs)
	assert.NotContains(t, string(logs), "Zebediah")
	ev := findEvent(t, logs, "task.failed")
	assert.Contains(t, ev["stderr_tail"], "simulated crash")
}

func TestUndecodableMediaIsNotRetryable(t *testing.T) {
	h := newHarness(t)
	h.cfg.ChildEnv = []string{workertest.FFmpegMode(workertest.ModeInvalid)}
	task := h.srv.AddTask(workertest.Task{Kind: api.KindPrepare, Input: []byte("not media")})
	_, _ = h.run()
	h.activate()
	h.eventually(h.taskState(task, api.TaskFailed), "task did not fail")
	f := h.srv.Task(task).Failures
	require.Len(t, f, 1)
	assert.False(t, f[0].Retryable)
	assert.Equal(t, api.StageChild, f[0].Stage)
	assert.Contains(t, f[0].StderrTail, "Invalid data found when processing input")
}

func TestUnparsableWhisperOutputIsRetryable(t *testing.T) {
	for _, mode := range []string{workertest.ModeBadJSON, workertest.ModeNoJSON} {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t)
			h.cfg.ChildEnv = []string{workertest.WhisperMode(mode)}
			task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(1), Model: "tiny.test"})
			_, _ = h.run()
			h.activate()
			h.eventually(func() bool { return len(h.srv.Task(task).Failures) > 0 }, "no failure")
			assert.True(t, h.srv.Task(task).Failures[0].Retryable)
		})
	}
}

func TestChildTimeout(t *testing.T) {
	h := newHarness(t)
	h.cfg.ChildEnv = []string{workertest.WhisperMode(workertest.ModeHang)}
	h.cfg.Limits.TimeoutFactor = 0.01
	h.cfg.Limits.TimeoutBase = 200 * time.Millisecond
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(1), Model: "tiny.test"})
	_, _ = h.run()
	h.activate()
	h.eventually(func() bool { return len(h.srv.Task(task).Failures) > 0 }, "no failure")
	f := h.srv.Task(task).Failures[0]
	assert.True(t, f.Retryable)
	assert.Contains(t, f.Reason, "whisper-cli timed out")
}

// An input_url on another origin is refused before any work: the
// worker's or server's configuration is at fault, not the task (#64).
func TestForeignInputURLIsPreWork(t *testing.T) {
	for _, kind := range []api.TaskKind{api.KindPrepare, api.KindTranscribe} {
		t.Run(string(kind), func(t *testing.T) {
			h := newHarness(t)
			task := h.srv.AddTask(workertest.Task{Kind: kind, Input: workertest.WAV(1), Model: "tiny.test",
				InputURL: "https://elsewhere.invalid/input"})
			_, _ = h.run()
			h.activate()
			h.eventually(func() bool { return len(h.srv.Task(task).Failures) > 0 }, "no failure")
			f := h.srv.Task(task).Failures[0]
			assert.Equal(t, api.StagePreWork, f.Stage)
			assert.True(t, f.Retryable)
			assert.Contains(t, f.Reason, "input_url")
		})
	}
}

func TestModelHashMismatchFailsTask(t *testing.T) {
	h := newHarness(t)
	h.srv.AddModel("tiny.test", []byte("tampered"))
	task := h.srv.AddTask(workertest.Task{Kind: api.KindTranscribe, Input: workertest.WAV(1), Model: "tiny.test"})
	_, _ = h.run()
	h.activate()
	h.eventually(func() bool { return len(h.srv.Task(task).Failures) > 0 }, "no failure")
	f := h.srv.Task(task).Failures[0]
	assert.True(t, f.Retryable)
	assert.Contains(t, f.Reason, "server pins sha256")
	assert.Equal(t, api.StagePreWork, f.Stage, "the worker's model table, not the task")
	_, err := os.Stat(filepath.Join(h.cfg.StateDir, "models", "ggml-tiny.test.bin"))
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestClaimsOfferOnlyWhatFitsInMemory(t *testing.T) {
	h := newHarness(t)
	h.cfg.Memory = 2 * gib // budget 1 GiB: big.test (3 GiB) does not fit
	_, _ = h.run()
	h.activate()
	h.eventually(func() bool { return len(h.srv.Claims()) > 0 }, "no claim")
	c := h.srv.Claims()[0]
	assert.Equal(t, []string{"tiny.test"}, c.Models)
	assert.ElementsMatch(t, []api.TaskKind{api.KindPrepare, api.KindTranscribe}, c.Kinds)
	w := h.worker()
	assert.Equal(t, []string{"tiny.test"}, w.Facts.Models)
}

func findEvent(t *testing.T, logs []byte, event string) map[string]any {
	t.Helper()
	for _, line := range bytes.Split(logs, []byte("\n")) {
		var m map[string]any
		if json.Unmarshal(line, &m) == nil && m["event"] == event {
			return m
		}
	}
	t.Fatalf("no %s event in logs:\n%s", event, logs)
	return nil
}
