package fetchpolicy

import (
	"sort"
	"time"
)

// State is the observed pacing state for one (site, egress) pair.
type State struct {
	Starts        []time.Time // recent fetch starts, at least the longest rule window
	InFlight      int
	CooldownUntil time.Time
}

// Decide reports whether a fetch may start at now. It is ok iff now is at or
// past the cooldown, InFlight < Concurrency, and every rule has fewer than N
// starts in (now-Window, now].
//
// When blocked, next is the earliest time at which all time-based blockers
// (cooldown and rate rules) have cleared, i.e. the latest of their clearing
// times. It is zero when the only blocker is concurrency, since that clears
// on an event rather than at a known time. A rule with N starts allowed and
// count in-window starts clears at the (count-N+1)-th oldest in-window start
// plus Window. Concurrency may still block at next.
func Decide(p Policy, s State, now time.Time) (ok bool, next time.Time) {
	ok = true
	if now.Before(s.CooldownUntil) {
		ok = false
		next = s.CooldownUntil
	}
	if s.InFlight >= p.Concurrency {
		ok = false
	}
	starts := append([]time.Time(nil), s.Starts...)
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	for _, r := range p.Rules {
		cutoff := now.Add(-r.Window)
		var in []time.Time
		for _, t := range starts {
			if t.After(cutoff) && !t.After(now) {
				in = append(in, t)
			}
		}
		if len(in) < r.N {
			continue
		}
		ok = false
		if c := in[len(in)-r.N].Add(r.Window); c.After(next) {
			next = c
		}
	}
	if ok {
		return true, time.Time{}
	}
	return false, next
}

// Backoff returns min(BackoffInitial*2^level, BackoffCap).
func Backoff(p Policy, level int) time.Duration {
	if level < 0 {
		level = 0
	}
	d := p.BackoffInitial
	for i := 0; i < level; i++ {
		if d >= p.BackoffCap || d > p.BackoffCap/2 {
			return p.BackoffCap
		}
		d *= 2
	}
	if d > p.BackoffCap {
		return p.BackoffCap
	}
	return d
}
