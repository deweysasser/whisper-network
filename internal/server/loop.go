package server

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/deweysasser/whisper-network/internal/api"
)

// Repetition-loop thresholds (issue #99). Whisper sometimes locks onto one
// short line and repeats it to the end of the task; the observed loops ran
// 289 to 1,914 identical segments in a row.
const (
	loopMaxRun   = 50   // more identical segments in a row than this is a loop
	loopMinCount = 50   // ... as is one line seen more often than this
	loopMaxShare = 0.30 // ... when it is also over this share of the segments
)

// errLoop describes a refused looping transcript. It carries counts only:
// its text becomes the task's error, which is logged and shown to anyone
// who can see the job, so it must never quote the transcript (DESIGN.md
// §10.1).
type errLoop struct {
	run, count, total int
}

func (e *errLoop) Error() string {
	return fmt.Sprintf("repetition loop: one line repeated %d times in a row, %d of %d segments",
		e.run, e.count, e.total)
}

// detectLoop reports a transcript stuck repeating one line: some line
// appears more than loopMaxRun times in a row, or more than loopMinCount
// times making up over loopMaxShare of the non-blank segments. Lines are
// compared after normalizeLine. It returns nil for a normal transcript.
func detectLoop(segs []api.Segment) *errLoop {
	counts := map[string]int{}
	runs := map[string]int{} // longest run per line
	var prev string
	run, total := 0, 0
	for _, sg := range segs {
		line := normalizeLine(sg.Text)
		if line == "" {
			continue
		}
		total++
		counts[line]++
		if line == prev {
			run++
		} else {
			prev, run = line, 1
		}
		runs[line] = max(runs[line], run)
	}
	var worst *errLoop
	for line, n := range counts {
		looped := runs[line] > loopMaxRun || n > loopMinCount && float64(n) > loopMaxShare*float64(total)
		if looped && (worst == nil || n > worst.count) {
			worst = &errLoop{run: runs[line], count: n, total: total}
		}
	}
	return worst
}

// normalizeLine folds case and whitespace and drops surrounding
// punctuation, so "Thank you." and " thank  you!" compare equal.
func normalizeLine(s string) string {
	s = strings.Join(strings.Fields(strings.ToLower(s)), " ")
	return strings.TrimFunc(s, func(r rune) bool { return unicode.IsPunct(r) || unicode.IsSpace(r) })
}
