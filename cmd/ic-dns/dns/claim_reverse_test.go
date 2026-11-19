package dns

import (
	"fmt"
	"net/netip"
	"testing"

	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// TestReverseZones pins the zones a prefix claims: the zone reverseName builds
// for an address in it when the prefix is that narrow or narrower, otherwise the
// zones of the octet (nibble) boundary inside it, never one reaching past it.
func TestReverseZones(t *testing.T) {
	t.Parallel()

	sixteen := make([]string, 0, 16)
	for i := range 16 {
		sixteen = append(sixteen, fmt.Sprintf("%d.147.10.in-addr.arpa.", i))
	}

	cases := []struct {
		prefix string
		want   []string
	}{
		{"10.146.11.0/24", []string{"11.146.10.in-addr.arpa."}},
		{"10.146.11.64/26", []string{"11.146.10.in-addr.arpa."}},
		{"10.146.11.7/32", []string{"11.146.10.in-addr.arpa."}},
		{"10.147.0.0/16", []string{"147.10.in-addr.arpa."}},
		{"10.147.0.0/20", sixteen},
		{"10.146.10.0/23", []string{"10.146.10.in-addr.arpa.", "11.146.10.in-addr.arpa."}},
		{"10.0.0.0/8", []string{"10.in-addr.arpa."}},
		{"0.0.0.0/0", []string{"in-addr.arpa."}},
		{"::ffff:10.146.11.0/120", []string{"11.146.10.in-addr.arpa."}},
		{"fd42:3d63:aaa1:de63::/64", []string{"3.6.e.d.1.a.a.a.3.6.d.3.2.4.d.f.ip6.arpa."}},
		{"fd42:3d63:aaa1:de63:1::/80", []string{"3.6.e.d.1.a.a.a.3.6.d.3.2.4.d.f.ip6.arpa."}},
		{"fd42:3d63::/32", []string{"3.6.d.3.2.4.d.f.ip6.arpa."}},
		{"fd42:3d63:aaa1:de60::/62", []string{
			"0.6.e.d.1.a.a.a.3.6.d.3.2.4.d.f.ip6.arpa.",
			"1.6.e.d.1.a.a.a.3.6.d.3.2.4.d.f.ip6.arpa.",
			"2.6.e.d.1.a.a.a.3.6.d.3.2.4.d.f.ip6.arpa.",
			"3.6.e.d.1.a.a.a.3.6.d.3.2.4.d.f.ip6.arpa.",
		}},
		{"fd42:3d63:aaa0::/46", []string{
			"0.a.a.a.3.6.d.3.2.4.d.f.ip6.arpa.",
			"1.a.a.a.3.6.d.3.2.4.d.f.ip6.arpa.",
			"2.a.a.a.3.6.d.3.2.4.d.f.ip6.arpa.",
			"3.a.a.a.3.6.d.3.2.4.d.f.ip6.arpa.",
		}},
	}

	for _, tc := range cases {
		got := reverseZones(netip.MustParsePrefix(tc.prefix).Masked())
		assert.Equal(t, tc.want, got, tc.prefix)
	}

	// The widest spread a prefix can take: seven bits short of an octet.
	assert.Len(t, reverseZones(netip.MustParsePrefix("10.146.0.0/17")), 128)

	assert.Nil(t, reverseZones(netip.Prefix{}))
}

// TestReverseZonesMatchWhatIsPublished pins that a claim names the zone an
// instance's PTR is published in, so the two never disagree on where it lives.
func TestReverseZonesMatchWhatIsPublished(t *testing.T) {
	t.Parallel()

	for _, text := range []string{"10.146.11.7", "10.147.11.200", "fd42:3d63:aaa1:de63::5", "fd42:3d63:aaa1:de63:216:3eff:fe00:1"} {
		addr := netip.MustParseAddr(text)

		_, zone, ok := reverseName(addr)
		require.True(t, ok, text)

		bits := 24
		if addr.Is6() {
			bits = 64
		}

		assert.Equal(t, []string{zone}, reverseZones(netip.PrefixFrom(addr, bits).Masked()), text)
		assert.Equal(t, []string{zone}, reverseZones(netip.PrefixFrom(addr, addr.BitLen())), text)
	}
}

// TestReverseClaimsListsEachZoneOnce pins that overlapping prefixes claim a zone
// once, in the order the prefixes came, and that none claims nothing.
func TestReverseClaimsListsEachZoneOnce(t *testing.T) {
	t.Parallel()

	got := reverseClaims([]netip.Prefix{
		netip.MustParsePrefix("10.146.11.0/26"),
		netip.MustParsePrefix("10.147.1.0/24"),
		netip.MustParsePrefix("10.146.11.128/25"),
	})

	assert.Equal(t, []string{"11.146.10.in-addr.arpa.", "1.147.10.in-addr.arpa."}, got)
	assert.Nil(t, reverseClaims(nil))
}

// TestClaimReverseFailsAColdPTR walks a start with and without --claim-reverse.
// Before the round ends, a PTR in a declared subnet that no instance read so far
// holds is SERVFAIL rather than the forwarder's NXDOMAIN, since the instance may
// only not have been read yet; a PTR outside every declared subnet is forwarded
// as ever. Once warm the claim changes nothing: the forwarder answers a subnet
// with no instance, and a published zone answers for itself.
func TestClaimReverseFailsAColdPTR(t *testing.T) {
	t.Parallel()

	v6Claimed, err := dns.ReverseAddr("fd42:3d63:aaa1:de63::5")
	require.NoError(t, err)

	const (
		v4Claimed = "7.11.146.10.in-addr.arpa."
		v4Wide    = "9.3.147.10.in-addr.arpa."
		outside   = "7.12.146.10.in-addr.arpa."
		public    = "8.8.8.8.in-addr.arpa."
		published = "2.0.0.10.in-addr.arpa."
		unread    = "9.0.0.10.in-addr.arpa."
	)

	claim := ClaimReverse([]netip.Prefix{
		netip.MustParsePrefix("10.146.11.0/24"),
		netip.MustParsePrefix("10.147.0.0/16"),
		netip.MustParsePrefix("fd42:3d63:aaa1:de63::/64"),
	})

	servfail := want{rcode: dns.RcodeServerFailure, forwarded: true}
	forwardedNX := want{rcode: dns.RcodeNameError, forwarded: true}

	cases := []struct {
		name string
		opts []Option
		cold map[string]want
	}{
		{
			name: "claimed",
			opts: []Option{Suffix("incus"), claim},
			cold: map[string]want{
				v4Claimed: servfail,
				v4Wide:    servfail,
				v6Claimed: servfail,
				outside:   forwardedNX,
				public:    forwardedNX,
				published: {rcode: dns.RcodeSuccess},
				unread:    {rcode: dns.RcodeServerFailure},
			},
		},
		{
			// What this fixes: the forwarder's NXDOMAIN reaches the client.
			name: "not claimed",
			opts: []Option{Suffix("incus")},
			cold: map[string]want{
				v4Claimed: forwardedNX,
				v4Wide:    forwardedNX,
				v6Claimed: forwardedNX,
				outside:   forwardedNX,
				public:    forwardedNX,
				published: {rcode: dns.RcodeSuccess},
				unread:    {rcode: dns.RcodeServerFailure},
			},
		},
	}

	warmCases := map[string]want{
		v4Claimed: forwardedNX,
		v4Wide:    forwardedNX,
		v6Claimed: forwardedNX,
		outside:   forwardedNX,
		public:    forwardedNX,
		published: {rcode: dns.RcodeSuccess},
		unread:    {rcode: dns.RcodeNameError},
	}

	for _, tc := range cases {
		c := newColdServer(t, tc.opts...)

		c.p.fold(enriched(incusapi.EventLifecycleInstanceStarted, "shop", "web", "10.0.0.2"))
		c.p.write()

		c.check(t, tc.name+", cold", tc.cold)

		c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))

		c.check(t, tc.name+", warm", warmCases)
	}

	// The suffix claim is kept alongside, and a root suffix still claims
	// nothing while the reverse zones are claimed all the same.
	c := newColdServer(t, Suffix("incus"), claim)
	assert.Contains(t, c.p.view.Claims, "incus.")

	c = newColdServer(t, Suffix("."), claim)
	assert.NotContains(t, c.p.view.Claims, ".")
	assert.Contains(t, c.p.view.Claims, "11.146.10.in-addr.arpa.")
}
