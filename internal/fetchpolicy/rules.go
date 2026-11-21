package fetchpolicy

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	MinWindow = time.Second
	MaxWindow = 168 * time.Hour
)

// Rule allows at most N fetch starts per sliding Window.
type Rule struct {
	N      int
	Window time.Duration
}

// ParseRule parses "N/DUR", e.g. "1/3m" or "30/1h".
func ParseRule(s string) (Rule, error) {
	n, d, ok := strings.Cut(strings.TrimSpace(s), "/")
	if !ok {
		return Rule{}, fmt.Errorf("rule %q: want N/DURATION", s)
	}
	cnt, err := strconv.Atoi(n)
	if err != nil {
		return Rule{}, fmt.Errorf("rule %q: bad count", s)
	}
	w, err := time.ParseDuration(d)
	if err != nil {
		return Rule{}, fmt.Errorf("rule %q: bad window", s)
	}
	r := Rule{N: cnt, Window: w}
	if err := r.validate(); err != nil {
		return Rule{}, fmt.Errorf("rule %q: %w", s, err)
	}
	return r, nil
}

func (r Rule) validate() error {
	if r.N < 1 {
		return errors.New("count must be >= 1")
	}
	if r.Window < MinWindow || r.Window > MaxWindow {
		return fmt.Errorf("window must be between %s and %s", MinWindow, MaxWindow)
	}
	return nil
}

// String formats the rule so ParseRule round-trips it.
func (r Rule) String() string { return fmt.Sprintf("%d/%s", r.N, r.Window) }

// Policy is one stored pacing policy. Key is Site, or Site+"@"+EgressKey when
// EgressKey is set. Site "*" matches any site.
type Policy struct {
	Key, Site, EgressKey string
	Rules                []Rule
	Concurrency          int
	BackoffInitial       time.Duration
	BackoffCap           time.Duration
}

// Validate checks the policy's invariants.
func (p Policy) Validate() error {
	if p.Concurrency < 1 {
		return errors.New("concurrency must be >= 1")
	}
	if p.BackoffInitial < time.Second {
		return errors.New("backoff initial must be >= 1s")
	}
	if p.BackoffCap < p.BackoffInitial {
		return errors.New("backoff cap must be >= initial")
	}
	for _, r := range p.Rules {
		if err := r.validate(); err != nil {
			return err
		}
	}
	if p.Site != "*" {
		got, err := SiteKey("https://" + hostForm(p.Site) + "/")
		if err != nil || got != p.Site {
			return fmt.Errorf("invalid site %q", p.Site)
		}
	}
	if strings.Contains(p.EgressKey, "@") {
		return errors.New("egress key must not contain '@'")
	}
	want := p.Site
	if p.EgressKey != "" {
		want = p.Site + "@" + p.EgressKey
	}
	if p.Key != want {
		return fmt.Errorf("key %q must be %q", p.Key, want)
	}
	return nil
}

// hostForm brackets IPv6 literals so a site key can be re-parsed as a URL host.
func hostForm(site string) string {
	if strings.Contains(site, ":") {
		return "[" + site + "]"
	}
	return site
}

// Resolve picks the most specific policy for (site, egressKey):
// site@egress > site > *@egress > *. It returns the zero Policy if none match.
func Resolve(ps []Policy, site, egressKey string) Policy {
	best, bestScore := Policy{}, 0
	for _, p := range ps {
		score := 0
		switch {
		case p.Site == site && p.EgressKey == egressKey && egressKey != "":
			score = 4
		case p.Site == site && p.EgressKey == "":
			score = 3
		case p.Site == "*" && p.EgressKey == egressKey && egressKey != "":
			score = 2
		case p.Site == "*" && p.EgressKey == "":
			score = 1
		}
		if score > bestScore {
			best, bestScore = p, score
		}
	}
	return best
}

// LongestWindow returns the largest rule window across ps (0 if none).
func LongestWindow(ps []Policy) time.Duration {
	var m time.Duration
	for _, p := range ps {
		for _, r := range p.Rules {
			if r.Window > m {
				m = r.Window
			}
		}
	}
	return m
}
