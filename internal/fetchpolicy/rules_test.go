package fetchpolicy

import (
	"testing"
	"time"
)

func TestParseRule(t *testing.T) {
	r, err := ParseRule("1/3m")
	if err != nil || r != (Rule{1, 3 * time.Minute}) {
		t.Fatalf("got %v, %v", r, err)
	}
	if r.String() != "1/3m" {
		t.Errorf("String = %q", r.String())
	}
	if r2, err := ParseRule(r.String()); err != nil || r2 != r {
		t.Errorf("round trip: %v %v", r2, err)
	}
	if r, err := ParseRule("30/1h"); err != nil || r != (Rule{30, time.Hour}) {
		t.Fatalf("got %v, %v", r, err)
	}
	for _, s := range []string{"0/1m", "1/500ms", "1/200h", "x/1m", "1", "1/", "/1m", "-1/1m", "1/x"} {
		if _, err := ParseRule(s); err == nil {
			t.Errorf("ParseRule(%q) want error", s)
		}
	}
}

func TestRuleStringCompact(t *testing.T) {
	for want, r := range map[string]Rule{
		"1/3m": {1, 3 * time.Minute}, "30/1h": {30, time.Hour}, "1/90s": {1, 90 * time.Second},
		"2/1h30m": {2, 90 * time.Minute}, "1/30s": {1, 30 * time.Second}, "5/168h": {5, 168 * time.Hour},
	} {
		if got := r.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
		if r2, err := ParseRule(want); err != nil || r2 != r {
			t.Errorf("ParseRule(%q) = %v, %v", want, r2, err)
		}
	}
	for _, s := range []string{"1/3m0s", "2/1h0m0s", "2/1h30m0s"} {
		if _, err := ParseRule(s); err != nil {
			t.Errorf("ParseRule(%q): %v", s, err)
		}
	}
}

func TestPolicyValidate(t *testing.T) {
	ok := Policy{Key: "youtube.com@label:home", Site: "youtube.com", EgressKey: "label:home", Concurrency: 1, BackoffInitial: time.Second, BackoffCap: time.Second}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	star := Policy{Key: "*", Site: "*", Concurrency: 2, BackoffInitial: 5 * time.Minute, BackoffCap: 6 * time.Hour}
	if err := star.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := map[string]func(p *Policy){
		"conc":     func(p *Policy) { p.Concurrency = 0 },
		"initial":  func(p *Policy) { p.BackoffInitial = time.Millisecond },
		"cap":      func(p *Policy) { p.BackoffCap = 0 },
		"site":     func(p *Policy) { p.Site = "www.a.b.example.com"; p.Key = p.Site + "@label:home" },
		"key":      func(p *Policy) { p.Key = "youtube.com" },
		"egress@":  func(p *Policy) { p.EgressKey = "a@b"; p.Key = "youtube.com@a@b" },
		"rule":     func(p *Policy) { p.Rules = []Rule{{0, time.Minute}} },
		"rulewin":  func(p *Policy) { p.Rules = []Rule{{1, time.Millisecond}} },
		"noegress": func(p *Policy) { p.EgressKey = ""; p.Key = "youtube.com@label:home" },
	}
	for name, f := range bad {
		p := ok
		f(&p)
		if p.Validate() == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestResolveSpecificity(t *testing.T) {
	ps := []Policy{
		{Key: "*", Site: "*"},
		{Key: "*@label:home", Site: "*", EgressKey: "label:home"},
		{Key: "youtube.com", Site: "youtube.com"},
		{Key: "youtube.com@label:home", Site: "youtube.com", EgressKey: "label:home"},
	}
	cases := []struct{ site, eg, want string }{
		{"youtube.com", "label:home", "youtube.com@label:home"},
		{"youtube.com", "label:other", "youtube.com"},
		{"example.com", "label:home", "*@label:home"},
		{"example.com", "label:other", "*"},
		{"example.com", "", "*"},
	}
	for _, c := range cases {
		if got := Resolve(ps, c.site, c.eg).Key; got != c.want {
			t.Errorf("Resolve(%s,%s) = %q want %q", c.site, c.eg, got, c.want)
		}
	}
	if got := Resolve(nil, "a.com", ""); got.Key != "" {
		t.Errorf("empty resolve = %+v", got)
	}
}

func TestLongestWindow(t *testing.T) {
	ps := []Policy{
		{Rules: []Rule{{1, time.Minute}, {3, time.Hour}}},
		{Rules: []Rule{{1, 5 * time.Minute}}},
		{},
	}
	if got := LongestWindow(ps); got != time.Hour {
		t.Errorf("got %v", got)
	}
	if LongestWindow(nil) != 0 {
		t.Error("nil")
	}
}
