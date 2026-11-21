package fetchpolicy

import "testing"

func TestSiteKeyAliases(t *testing.T) {
	for _, u := range []string{
		"https://youtu.be/x",
		"https://m.youtube.com/watch?v=x",
		"https://music.youtube.com/watch?v=x",
		"https://www.youtube-nocookie.com/embed/x",
		"https://YouTube.com./x",
	} {
		got, err := SiteKey(u)
		if err != nil || got != "youtube.com" {
			t.Errorf("SiteKey(%q) = %q, %v", u, got, err)
		}
	}
}

func TestSiteKeyRegistrable(t *testing.T) {
	got, err := SiteKey("http://a.b.example.co.uk/p")
	if err != nil || got != "example.co.uk" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestSiteKeyIP(t *testing.T) {
	for in, want := range map[string]string{
		"http://203.0.113.7/x":      "203.0.113.7",
		"http://[2001:DB8::1]:80/x": "2001:db8::1",
	} {
		got, err := SiteKey(in)
		if err != nil || got != want {
			t.Errorf("SiteKey(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
}

func TestSiteKeyRejects(t *testing.T) {
	for _, u := range []string{"ftp://example.com/x", "https://user:pw@h.example.com/", "https:///x", "not a url", "https://localhost/x", ""} {
		if got, err := SiteKey(u); err == nil {
			t.Errorf("SiteKey(%q) = %q, want error", u, got)
		}
	}
}
