package egress

import (
	"net/netip"
	"testing"
)

func TestRefusedRanges(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1": true, "10.1.2.3": true, "172.16.0.1": true, "192.168.1.1": true,
		"100.64.0.1": true, "169.254.169.254": true, "::1": true, "fe80::1": true,
		"fd00::1": true, "::ffff:10.0.0.1": true, "224.0.0.1": true, "0.0.0.0": true,
		"::": true, "0.1.2.3": true, "255.255.255.255": true, "ff02::1": true,
		"192.0.2.1": true, "198.51.100.1": true, "203.0.113.1": true, "2001:db8::1": true,
		"64:ff9b::a00:1": true, "2002:0a00:0001::1": true, "::ffff:8.8.8.8": false,
		"64:ff9b::808:808": false, "2002:0808:0808::1": false,
		"::7f00:1": true, "::a00:1": true, "64:ff9b:1::1": true, "2001::1": true,
		"100::1": true, "fec0::1": true, "::808:808": true,
		"8.8.8.8": false, "2606:4700::1": false,
	}
	for s, want := range cases {
		if got := Refused(netip.MustParseAddr(s)); got != want {
			t.Errorf("Refused(%s)=%v want %v", s, got, want)
		}
	}
	if !Refused(netip.Addr{}) {
		t.Error("invalid addr must be refused")
	}
}
