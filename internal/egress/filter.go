package egress

import "net/netip"

var refusedPrefixes = func() []netip.Prefix {
	var out []netip.Prefix
	for _, s := range []string{
		"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16",
		"172.16.0.0/12", "192.168.0.0/16", "192.0.0.0/24", "192.0.2.0/24",
		"198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
		"::/96", "::1/128", "100::/64", "64:ff9b:1::/48", "2001::/32", "fec0::/10", "fc00::/7", "fe80::/10", "ff00::/8", "2001:db8::/32",
	} {
		out = append(out, netip.MustParsePrefix(s))
	}
	return out
}()

var (
	nat64  = netip.MustParsePrefix("64:ff9b::/96")
	sixto4 = netip.MustParsePrefix("2002::/16")
)

// Refused reports whether a must never be dialed by a downloader: loopback,
// RFC 1918, CGNAT, link-local, ULA, multicast, unspecified, reserved,
// documentation ranges, and IPv4 forms embedded in IPv4-mapped, NAT64 and
// 6to4 addresses. An invalid address is refused.
func Refused(a netip.Addr) bool {
	if !a.IsValid() {
		return true
	}
	a = a.WithZone("").Unmap()
	if a.Is6() {
		b := a.As16()
		switch {
		case nat64.Contains(a):
			return Refused(netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}))
		case sixto4.Contains(a):
			return Refused(netip.AddrFrom4([4]byte{b[2], b[3], b[4], b[5]}))
		}
	}
	for _, p := range refusedPrefixes {
		if p.Contains(a) {
			return true
		}
	}
	return false
}
