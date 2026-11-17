package worker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/media"
	"github.com/deweysasser/whisper-network/internal/models"
	"github.com/deweysasser/whisper-network/internal/sandbox"
)

// TranscribeParams are the inputs of one whisper-cli run.
type TranscribeParams struct {
	Model, Input, OutBase string
	Language              string
	Threads               int
	Prompt                string
	VADModel              string // path; "" disables VAD
	NoContext             bool   // loop retry: -mc 0 and no prompt
}

// TranscribeArgs returns whisper-cli's arguments (without argv[0]), one per
// element, as verified against examples/cli/cli.cpp of whisper.cpp v1.9.5:
// -m model, -f input, -l language, -t threads, -oj with -of base (writes
// base.json), -np to silence progress prints, --prompt, and --vad with
// -vm for voice activity detection.
func TranscribeArgs(p TranscribeParams) []string {
	args := []string{"-m", p.Model, "-f", p.Input, "-l", p.Language,
		"-t", strconv.Itoa(p.Threads), "-oj", "-of", p.OutBase, "-np"}
	if p.NoContext {
		// A repetition-loop retry: -mc 0 stops conditioning on previous
		// text, and the prompt, which feeds the same context, is dropped.
		args = append(args, "-mc", "0")
	} else if p.Prompt != "" {
		args = append(args, "--prompt", oneLine(p.Prompt))
	}
	if p.VADModel != "" {
		args = append(args, "--vad", "-vm", p.VADModel)
	}
	return args
}

// responseFile renders args as a whisper-cli response file: invoked as
// "whisper-cli @file", it reads one argument per line (cli.cpp main, the
// argc == 2 '@' branch). The worker passes arguments this way so the prompt,
// which can name meeting participants, never appears in the process table.
func responseFile(args []string) []byte {
	var b strings.Builder
	for _, a := range args {
		b.WriteString(oneLine(a))
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// oneLine replaces line breaks, which a response file cannot carry.
func oneLine(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' {
			return ' '
		}
		return r
	}, s)
}

// transcribe runs whisper-cli on the lease's WAV and uploads the segments.
func (w *Worker) transcribe(ctx context.Context, l *api.Lease, dir string, run *leaseRun, log zerolog.Logger) (taskStats, error) {
	if w.tools.whisper == "" {
		return taskStats{}, preWork("whisper-cli is not installed on this worker", nil)
	}
	if l.AudioSamples <= 0 {
		// The size bounds the download, the timeout and RLIMIT_AS; without
		// it the input would be unbounded.
		return taskStats{}, &taskError{reason: "lease carries no audio_samples; refusing an unbounded input",
			stage: api.StageInput}
	}
	if err := w.checkInputURL(l); err != nil {
		return taskStats{}, err
	}
	m, err := w.leaseModel(l.Model, l.ModelSHA256, models.KindWhisper)
	if err != nil {
		return taskStats{}, err
	}
	modelPath, releaseModel, err := w.cache.Acquire(ctx, m)
	if err != nil {
		return taskStats{}, staged(api.StagePreWork, transferError("fetching model "+m.Name, err))
	}
	defer releaseModel() // held against cache eviction while whisper-cli runs
	w.disk.invalidate()
	var vadPath string
	if l.VADModel != "" {
		vm, err := w.leaseModel(l.VADModel, l.VADModelSHA256, models.KindVAD)
		if err != nil {
			return taskStats{}, err
		}
		var releaseVAD func()
		if vadPath, releaseVAD, err = w.cache.Acquire(ctx, vm); err != nil {
			return taskStats{}, staged(api.StagePreWork, transferError("fetching VAD model "+vm.Name, err))
		}
		defer releaseVAD()
	}

	in := filepath.Join(dir, "input.wav")
	maxIn := int64(len(media.WAVHeader(0))) + l.AudioSamples*media.BytesPerSample
	if _, err := w.download(ctx, l, in, maxIn, log); err != nil {
		return taskStats{}, staged(api.StageInput, transferError("downloading input", err))
	}
	stats, err := w.transcribeChild(ctx, l, dir, in, m, modelPath, vadPath, run, log)
	return stats, staged(api.StageChild, err)
}

// transcribeChild runs whisper-cli on the downloaded input and uploads the
// result.
func (w *Worker) transcribeChild(ctx context.Context, l *api.Lease, dir, in string, m models.Model,
	modelPath, vadPath string, run *leaseRun, log zerolog.Logger) (taskStats, error) {
	audio := time.Duration(l.AudioSamples) * time.Second / media.SampleRate

	lang := l.Language
	if lang == "" {
		lang = "en"
	}
	outBase := filepath.Join(dir, "out")
	rsp := filepath.Join(dir, "args.rsp")
	args := TranscribeArgs(TranscribeParams{Model: modelPath, Input: in, OutBase: outBase, Language: lang,
		Threads: w.cfg.ThreadsPerSlot, Prompt: l.Prompt, VADModel: vadPath, NoContext: l.NoContext})
	if err := w.writeForChild(rsp, responseFile(args)); err != nil {
		return taskStats{}, retryableErr("writing arguments", err)
	}

	lim := w.cfg.Limits
	ro := slices.Concat(systemReadOnly, childSystemFiles, w.tools.whisperRO, []string{modelPath})
	if vadPath != "" {
		ro = append(ro, vadPath)
	}
	spec := sandbox.Spec{
		ReadOnly: ro, ReadWrite: []string{dir}, DenyNet: true,
		FileSize: uint64(lim.TranscribeFileSize), OpenFiles: uint64(lim.OpenFiles),
		UID: w.cfg.ChildUID, GID: w.cfg.ChildGID,
	}.WithLowestPriority()
	w.limitProcesses(&spec)
	if !lim.NoAddressSpaceLimit {
		spec.AddressSpace = uint64(2*m.PeakRSS + m.Size + lim.AddressSpaceHeadroom)
	}
	timeout := time.Duration(lim.TimeoutFactor*float64(audio)) + lim.TimeoutBase
	res, err := w.runChild(ctx, childRun{argv: []string{w.tools.whisper, "@" + rsp}, spec: spec, dir: dir, timeout: timeout})
	if err != nil {
		return taskStats{}, childFailure("whisper-cli", res, err, nil)
	}

	raw, err := readOutput(outBase+".json", maxWhisperJSON)
	switch {
	case errors.Is(err, errBadOutput):
		return taskStats{}, &taskError{reason: "whisper-cli output refused", stderr: res.stderr, err: err}
	case err != nil:
		return taskStats{}, &taskError{reason: "whisper-cli wrote no JSON output", stderr: res.stderr, retryable: true, err: err}
	}
	segs, detected, err := parseWhisperJSON(raw, audio.Milliseconds())
	if err != nil {
		return taskStats{}, &taskError{reason: "unreadable whisper-cli output", stderr: res.stderr, retryable: true, err: err}
	}
	if detected == "" {
		detected = lang
	}
	result := &api.TranscribeResult{
		EngineVersion: w.tools.engineVersion, Model: m.Name, ModelSHA256: m.SHA256, VAD: l.VADModel,
		Language: detected, ComputeMS: res.elapsed.Milliseconds(), Segments: segs,
	}
	run.uploading.Store(true)
	err = w.putWithRetry(ctx, log, func(ctx context.Context) error {
		return w.api.PutTranscribeResult(ctx, l.ID, result)
	})
	if err != nil {
		return taskStats{}, transferError("uploading result", err)
	}
	return taskStats{model: m.Name, computeMS: result.ComputeMS, audioS: audio.Seconds()}, nil
}

// leaseModel finds the model a lease names in the pinned catalog and checks
// that the server pinned the same bytes.
func (w *Worker) leaseModel(name, sha string, kind models.Kind) (models.Model, error) {
	m, ok := w.cfg.Catalog.Lookup(name)
	if !ok || m.Kind != kind {
		return m, preWork(fmt.Sprintf("model %q is not in this worker's %s table", name, kind), nil)
	}
	if sha != "" && sha != m.SHA256 {
		return m, preWork(fmt.Sprintf("model %s: server pins sha256 %s, worker pins %s", name, sha, m.SHA256), nil)
	}
	return m, nil
}
