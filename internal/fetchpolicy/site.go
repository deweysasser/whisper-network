package fetchpolicy

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"

	"golang.org/x/net/publicsuffix"
)

var siteAliases = map[string]string{
	"youtu.be":             "youtube.com",
	"youtube-nocookie.com": "youtube.com",
}

// SiteKey returns the pacing key for a URL: the registrable domain (eTLD+1)
// of its host, lowercased, with known aliases folded together. IP literals
// yield their canonical string. Only http(s) URLs without userinfo and with a
// non-empty host are accepted.
func SiteKey(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", errors.New("invalid url")
	}
	if s := strings.ToLower(u.Scheme); s != "http" && s != "https" {
		return "", errors.New("url scheme must be http or https")
	}
	if u.User != nil {
		return "", errors.New("url must not contain userinfo")
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "" {
		return "", errors.New("url has no host")
	}
	if i := strings.IndexByte(host, '%'); i >= 0 {
		host = host[:i] // IPv6 zone
	}
	if ip := net.ParseIP(host); ip != nil {
		if a, ok := netip.AddrFromSlice(ip); ok {
			return a.Unmap().String(), nil
		}
		return ip.String(), nil
	}
	site, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err != nil {
		return "", fmt.Errorf("no registrable domain in host: %w", err)
	}
	if a, ok := siteAliases[site]; ok {
		site = a
	}
	return site, nil
}
