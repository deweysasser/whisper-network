package fetchpolicy

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func pol(conc int, rules ...Rule) Policy {
	return Policy{Key: "*", Site: "*", Rules: rules, Concurrency: conc, BackoffInitial: time.Second, BackoffCap: time.Hour}
}

func TestDecideRateRules(t *testing.T) {
	p := pol(5, Rule{1, 3 * time.Minute})
	s := State{Starts: []time.Time{t0}}
	ok, next := Decide(p, s, t0.Add(2*time.Minute+59*time.Second))
	if ok || !next.Equal(t0.Add(3*time.Minute)) {
		t.Fatalf("ok=%v next=%v", ok, next)
	}
	if ok, next := Decide(p, s, t0.Add(3*time.Minute)); !ok || !next.IsZero() {
		t.Fatalf("ok=%v next=%v", ok, next)
	}
	if ok, _ := Decide(p, State{}, t0); !ok {
		t.Fatal("empty state blocked")
	}
}

func TestDecideStackedRules(t *testing.T) {
	p := pol(5, Rule{2, time.Minute}, Rule{3, time.Hour})
	// three starts within the hour; two within the last minute.
	s := State{Starts: []time.Time{t0, t0.Add(30 * time.Minute), t0.Add(30*time.Minute + 10*time.Second)}}
	now := t0.Add(30*time.Minute + 20*time.Second)
	ok, next := Decide(p, s, now)
	// 2/1m clears at 30:00+1m = 31:00; 3/1h clears at t0+1h. Both must clear.
	if ok || !next.Equal(t0.Add(time.Hour)) {
		t.Fatalf("ok=%v next=%v", ok, next)
	}
	// only the minute rule blocked
	s = State{Starts: []time.Time{t0.Add(30 * time.Minute), t0.Add(30*time.Minute + 10*time.Second)}}
	ok, next = Decide(p, s, now)
	if ok || !next.Equal(t0.Add(31*time.Minute)) {
		t.Fatalf("ok=%v next=%v", ok, next)
	}
	// count exceeds N: 3 starts in 2/1m window -> clears when 2nd-oldest... (count-N+1)=2nd oldest + W
	s = State{Starts: []time.Time{t0, t0.Add(10 * time.Second), t0.Add(20 * time.Second)}}
	ok, next = Decide(pol(5, Rule{2, time.Minute}), s, t0.Add(30*time.Second))
	if ok || !next.Equal(t0.Add(10*time.Second+time.Minute)) {
		t.Fatalf("ok=%v next=%v", ok, next)
	}
}

func TestDecideConcurrency(t *testing.T) {
	p := pol(2)
	if ok, next := Decide(p, State{InFlight: 2}, t0); ok || !next.IsZero() {
		t.Fatalf("ok=%v next=%v", ok, next)
	}
	if ok, _ := Decide(p, State{InFlight: 1}, t0); !ok {
		t.Fatal("blocked")
	}
	// concurrency plus time-based blocker: next is the time blocker's clearing time.
	p = pol(1, Rule{1, time.Minute})
	ok, next := Decide(p, State{InFlight: 1, Starts: []time.Time{t0}}, t0.Add(10*time.Second))
	if ok || !next.Equal(t0.Add(time.Minute)) {
		t.Fatalf("ok=%v next=%v", ok, next)
	}
}

func TestDecideCooldown(t *testing.T) {
	p := pol(2)
	s := State{CooldownUntil: t0.Add(time.Hour)}
	ok, next := Decide(p, s, t0)
	if ok || !next.Equal(t0.Add(time.Hour)) {
		t.Fatalf("ok=%v next=%v", ok, next)
	}
	if ok, _ := Decide(p, s, t0.Add(time.Hour)); !ok {
		t.Fatal("blocked at cooldown end")
	}
	// cooldown later than rule: next is the latest.
	p = pol(2, Rule{1, time.Minute})
	s.Starts = []time.Time{t0}
	if _, next := Decide(p, s, t0.Add(time.Second)); !next.Equal(t0.Add(time.Hour)) {
		t.Fatalf("next=%v", next)
	}
}

func TestBackoffCaps(t *testing.T) {
	p := Policy{BackoffInitial: 12 * time.Hour, BackoffCap: 24 * time.Hour}
	for lvl, want := range []time.Duration{12 * time.Hour, 24 * time.Hour, 24 * time.Hour} {
		if got := Backoff(p, lvl); got != want {
			t.Errorf("level %d: %v want %v", lvl, got, want)
		}
	}
	if got := Backoff(p, 200); got != 24*time.Hour {
		t.Errorf("overflow: %v", got)
	}
	if got := Backoff(p, -1); got != 12*time.Hour {
		t.Errorf("negative: %v", got)
	}
}
