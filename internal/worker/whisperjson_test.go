package worker

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
	"github.com/deweysasser/whisper-network/internal/worker/workertest"
)

// testdata/whisper-v1.9.5.json follows output_json in examples/cli/cli.cpp of
// whisper.cpp v1.9.5, including its quirks: tab indentation, only '"' and
// '\' escaped (a raw tab sits inside the second segment's text), an empty
// segment, and a segment that starts before its predecessor and overruns
// the audio.
func TestParseWhisperJSONGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/whisper-v1.9.5.json")
	require.NoError(t, err)
	segs, lang, err := parseWhisperJSON(raw, 30000)
	require.NoError(t, err)
	assert.Equal(t, "en", lang)
	assert.Equal(t, []api.Segment{
		{StartMS: 0, EndMS: 4120, Text: "The meeting will come to order."},
		{StartMS: 4120, EndMS: 9000, Text: `Councilor "Smith" moved to approve \ the minutes.`},
		{StartMS: 8500, EndMS: 30000, Text: "[BLANK_AUDIO]"},
	}, segs)
}

func TestParseWhisperJSONMonotonic(t *testing.T) {
	raw := []byte(`{"result":{"language":"de"},"transcription":[
		{"offsets":{"from":5000,"to":6000},"text":" a"},
		{"offsets":{"from":4000,"to":3000},"text":" b"},
		{"offsets":{"from":-10,"to":7000},"text":" c"}]}`)
	segs, lang, err := parseWhisperJSON(raw, 0)
	require.NoError(t, err)
	assert.Equal(t, "de", lang)
	assert.Equal(t, []api.Segment{
		{StartMS: 5000, EndMS: 6000, Text: "a"},
		{StartMS: 5000, EndMS: 5000, Text: "b"},
		{StartMS: 5000, EndMS: 7000, Text: "c"},
	}, segs)
}

func TestParseWhisperJSONFakeMatchesParser(t *testing.T) {
	segs, _, err := parseWhisperJSON(workertest.WhisperJSON("m", "en", 7300, 2000), 7300)
	require.NoError(t, err)
	require.Len(t, segs, 4)
	assert.Equal(t, api.Segment{StartMS: 6000, EndMS: 7300, Text: "segment 3"}, segs[3])
}

func TestParseWhisperJSONErrors(t *testing.T) {
	_, _, err := parseWhisperJSON([]byte("{\n\t\"transcription\": [\n"), 0)
	assert.Error(t, err)
}

func TestSanitizeWhisperJSON(t *testing.T) {
	in := "{\"a\":\"x\ty\\\"\nz\"}\n"
	assert.Equal(t, "{\"a\":\"x y\\\" z\"}\n", string(sanitizeWhisperJSON([]byte(in))))
}

func TestTranscribeArgs(t *testing.T) {
	args := TranscribeArgs(TranscribeParams{Model: "/m.bin", Input: "/in.wav", OutBase: "/out", Language: "auto",
		Threads: 4, Prompt: "line one\nline two", VADModel: "/vad.bin"})
	assert.Equal(t, []string{"-m", "/m.bin", "-f", "/in.wav", "-l", "auto", "-t", "4", "-oj", "-of", "/out", "-np",
		"--prompt", "line one line two", "--vad", "-vm", "/vad.bin"}, args)
	assert.Equal(t, "-m\n/m.bin\n", string(responseFile(args[:2])))
	assert.NotContains(t, TranscribeArgs(TranscribeParams{Threads: 1}), "--prompt")
	assert.NotContains(t, TranscribeArgs(TranscribeParams{Threads: 1}), "-mc")
}

// TestTranscribeArgsNoContext: a loop retry runs with no previous-text
// conditioning (-mc 0) and no prompt, even if the lease carried one (#99).
func TestTranscribeArgsNoContext(t *testing.T) {
	args := TranscribeArgs(TranscribeParams{Model: "/m.bin", Input: "/in.wav", OutBase: "/out", Language: "en",
		Threads: 2, Prompt: "names", NoContext: true})
	assert.Equal(t, []string{"-m", "/m.bin", "-f", "/in.wav", "-l", "en", "-t", "2", "-oj", "-of", "/out", "-np",
		"-mc", "0"}, args)
}
