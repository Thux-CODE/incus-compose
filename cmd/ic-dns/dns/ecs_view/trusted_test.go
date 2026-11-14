package ecs_view

import (
	"context"
	"log/slog"
	"net/netip"
	"testing"

	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/plugin/test"
	"github.com/miekg/dns"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// router is the querier this file is about: an address on none of the worked
// topology's networks, which without trust is the fail-closed case.
const router = "10.99.0.1"

// trustedEngine serves the worked topology and exempts the given CIDRs.
func trustedEngine(t *testing.T, trusted ...string) *ECSView {
	t.Helper()

	v := engineWith(t)

	for _, cidr := range trusted {
		v.Trusted = append(v.Trusted, netip.MustParsePrefix(cidr))
	}

	return v
}

// answerFrom runs a prepared query arriving from an address, so a test can put
// a client subnet in the query and still choose where the packet came from.
func answerFrom(t *testing.T, v *ECSView, m *dns.Msg, from string) (bool, *dns.Msg) {
	t.Helper()

	w := dnstest.NewRecorder(&test.ResponseWriter{RemoteIP: from})

	handled, _, err := v.Answer(context.Background(), w, m)
	require.NoError(t, err)

	return handled, w.Msg
}

// askFrom is one question arriving from an address with no client subnet in it,
// which is how a client naming this server in resolv.conf asks.
func askFrom(t *testing.T, v *ECSView, qname string, qtype uint16, from string) (bool, *dns.Msg) {
	t.Helper()

	m := new(dns.Msg)
	m.SetQuestion(qname, qtype)

	return answerFrom(t, v, m, from)
}

// addrsOf is an answer's A records as text.
func addrsOf(t *testing.T, m *dns.Msg) []string {
	t.Helper()

	out := make([]string, 0, len(m.Answer))

	for _, rr := range m.Answer {
		a, ok := rr.(*dns.A)
		require.True(t, ok, "an A query is answered with A records")

		out = append(out, a.A.String())
	}

	return out
}

// TestTrustedClientIsAnsweredFromEveryView is the case the flag exists for: a
// router reaching every network is told about every network, including two that
// share nothing with each other.
func TestTrustedClientIsAnsweredFromEveryView(t *testing.T) {
	t.Parallel()

	v := trustedEngine(t, router+"/32")

	cases := []struct {
		name  string
		qname string
		want  []string
	}{
		{
			name:  "a name only api-net holds",
			qname: "gateway.shop.incus.",
			want:  []string{"10.0.1.10"},
		},
		{
			name:  "a name only user-net holds, which api-net cannot see",
			qname: "user-db.shop.incus.",
			want:  []string{"10.0.2.30"},
		},
		{
			name:  "a name on both wires answers each address once",
			qname: "user-api.shop.incus.",
			want:  []string{"10.0.1.20", "10.0.2.20"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handled, m := askFrom(t, v, tc.qname, dns.TypeA, router)

			require.True(t, handled)
			require.Equal(t, dns.RcodeSuccess, m.Rcode)

			// Length as well as membership: the views overlap, and the union is
			// deduplicated rather than repeated.
			assert.Len(t, m.Answer, len(tc.want))
			assert.ElementsMatch(t, tc.want, addrsOf(t, m))
		})
	}
}

// TestUntrustedQuerierIsUnchanged pins that nothing moves for everybody else:
// the same address, off the list, is still placed on no network and denied.
func TestUntrustedQuerierIsUnchanged(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		trusted []string
	}{
		{
			name: "an empty list trusts nobody",
		},
		{
			name:    "and a list somebody else is on does not stretch",
			trusted: []string{"10.98.0.0/24"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v := trustedEngine(t, tc.trusted...)

			handled, m := askFrom(t, v, "user-db.shop.incus.", dns.TypeA, router)

			require.True(t, handled)
			assert.Equal(t, dns.RcodeNameError, m.Rcode)
			assert.Empty(t, m.Answer)

			// And a querier that is on a network still sees only that network.
			handled, m = askFrom(t, v, "user-db.shop.incus.", dns.TypeA, "10.0.1.10")

			require.True(t, handled)
			assert.Equal(t, dns.RcodeNameError, m.Rcode)
		})
	}
}

// TestTrustIsTheSourceAddress pins which of the two identification channels
// grants the exemption. A client subnet is what a querier says about itself, so
// it may narrow an answer and never widen one.
func TestTrustIsTheSourceAddress(t *testing.T) {
	t.Parallel()

	v := trustedEngine(t, router+"/32")

	t.Run("a trusted source is trusted whatever subnet the query carries", func(t *testing.T) {
		t.Parallel()

		// The subnet names a querier on api-net alone, which cannot see user-db.
		handled, m := answerFrom(t, v, query("user-db.shop.incus.", dns.TypeA, "10.0.1.10"), router)

		require.True(t, handled)
		require.Equal(t, dns.RcodeSuccess, m.Rcode)
		assert.Equal(t, []string{"10.0.2.30"}, addrsOf(t, m))
	})

	t.Run("and a subnet naming a trusted address grants nothing", func(t *testing.T) {
		t.Parallel()

		// The packet comes from somewhere else entirely; only the option claims
		// to be the router.
		handled, m := answerFrom(t, v, query("user-db.shop.incus.", dns.TypeA, router), "10.240.0.1")

		require.True(t, handled)
		assert.Equal(t, dns.RcodeNameError, m.Rcode)
		assert.Empty(t, m.Answer)
	})
}

// TestTrustedClientStillGetsTheOtherAnswers pins that the exemption is from the
// view filter and from nothing else: a missing name, a missing type and a name
// outside every zone read exactly as they do for anybody.
func TestTrustedClientStillGetsTheOtherAnswers(t *testing.T) {
	t.Parallel()

	v := trustedEngine(t, router+"/32")

	cases := []struct {
		name    string
		qname   string
		qtype   uint16
		handled bool
		rcode   int
		answers int
		soa     bool
	}{
		{
			name:    "a name no view holds is NXDOMAIN",
			qname:   "nope.shop.incus.",
			qtype:   dns.TypeA,
			handled: true,
			rcode:   dns.RcodeNameError,
			soa:     true,
		},
		{
			name:    "a type no view holds for a name that exists is NODATA",
			qname:   "user-api.shop.incus.",
			qtype:   dns.TypeMX,
			handled: true,
			rcode:   dns.RcodeSuccess,
			soa:     true,
		},
		{
			name:    "a name outside every zone falls through to the forwarder",
			qname:   "github.com.",
			qtype:   dns.TypeA,
			handled: false,
		},
		{
			// The apex is answered above the filter, so trust changes nothing
			// about it either.
			name:    "and the apex is the zone's, not a view's",
			qname:   "shop.incus.",
			qtype:   dns.TypeSOA,
			handled: true,
			rcode:   dns.RcodeSuccess,
			answers: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handled, m := askFrom(t, v, tc.qname, tc.qtype, router)

			require.Equal(t, tc.handled, handled)

			if !tc.handled {
				return
			}

			assert.Equal(t, tc.rcode, m.Rcode)
			assert.Len(t, m.Answer, tc.answers, "a denial and a NODATA answer nothing")

			if tc.soa {
				require.Len(t, m.Ns, 1)
				assert.IsType(t, &dns.SOA{}, m.Ns[0])
			}
		})
	}
}

// aliasPiece is one name two instances claim as an alias, each from its own
// wire: the collision the engine refuses rather than picks a winner for.
func aliasPiece(target ...string) *Snapshot {
	views := []ViewID{apiView, userView}

	answer := ViewAnswer{}

	for i, name := range target {
		answer[views[i]] = Records{dns.TypeCNAME: []dns.RR{&dns.CNAME{
			Hdr:    dns.RR_Header{Name: "api.shop.incus.", Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: testTTL},
			Target: name,
		}}}
	}

	return newBuilder().
		zone("shop.incus.", false).
		name("api.shop.incus.", answer).
		at("10.0.1.0/24", apiView).
		at("10.0.2.0/24", userView).
		done()
}

// TestTrustedClientRefusesTwoClaimantsOfOneAlias pins that the union does not
// invent an answer a name may not hold: a name carries one CNAME, so two of
// them read as missing here as they do inside a single view.
func TestTrustedClientRefusesTwoClaimantsOfOneAlias(t *testing.T) {
	t.Parallel()

	t.Run("two claimants read as missing", func(t *testing.T) {
		t.Parallel()

		v := New(slog.Default())
		v.Replace(aliasPiece("gateway.shop.incus.", "user-api.shop.incus."))
		v.SetHealthy(true)
		v.Trusted = []netip.Prefix{netip.MustParsePrefix(router + "/32")}

		handled, m := askFrom(t, v, "api.shop.incus.", dns.TypeCNAME, router)

		require.True(t, handled)
		assert.Equal(t, dns.RcodeNameError, m.Rcode)
		assert.Empty(t, m.Answer)
	})

	t.Run("two claimants before warm are not ready", func(t *testing.T) {
		t.Parallel()

		// The claimant that has to go may only not have been read yet, so the
		// miss is SERVFAIL here as it is for an untrusted querier: an NXDOMAIN
		// would be final for the router and wrong once the round ends.
		v := New(slog.Default())
		v.Replace(aliasPiece("gateway.shop.incus.", "user-api.shop.incus."))
		v.Trusted = []netip.Prefix{netip.MustParsePrefix(router + "/32")}

		m := new(dns.Msg)
		m.SetQuestion("api.shop.incus.", dns.TypeCNAME)
		m.SetEdns0(1232, false)

		handled, got := answerFrom(t, v, m, router)

		require.True(t, handled)
		assert.Equal(t, dns.RcodeToString[dns.RcodeServerFailure], dns.RcodeToString[got.Rcode])
		assert.Empty(t, got.Answer)

		opt := got.IsEdns0()
		require.NotNil(t, opt)
		require.Len(t, opt.Option, 1)
		assert.Equal(t, dns.ExtendedErrorCodeNotReady, opt.Option[0].(*dns.EDNS0_EDE).InfoCode)

		// And without EDNS no OPT record comes back.
		_, plain := askFrom(t, v, "api.shop.incus.", dns.TypeCNAME, router)
		assert.Equal(t, dns.RcodeServerFailure, plain.Rcode)
		assert.Nil(t, plain.IsEdns0())
	})

	t.Run("one claimant on one wire answers the trusted querier", func(t *testing.T) {
		t.Parallel()

		v := New(slog.Default())
		v.Replace(aliasPiece("gateway.shop.incus."))
		v.SetHealthy(true)
		v.Trusted = []netip.Prefix{netip.MustParsePrefix(router + "/32")}

		handled, m := askFrom(t, v, "api.shop.incus.", dns.TypeCNAME, router)

		require.True(t, handled)
		require.Equal(t, dns.RcodeSuccess, m.Rcode)
		require.Len(t, m.Answer, 1)
		assert.Equal(t, "gateway.shop.incus.", m.Answer[0].(*dns.CNAME).Target)
	})
}

// TestTrustedAnswersAreCounted pins the result the exemption publishes under, so
// a deployment can tell a trusted answer from a filtered one.
func TestTrustedAnswersAreCounted(t *testing.T) {
	// Not t.Parallel(): requestCount is package-level, and the label is this
	// test's own server name so nothing else moves this series.
	const server = "metrics-trusted"

	v := trustedEngine(t, router+"/32")
	v.Metrics = true
	v.Server = server

	t.Cleanup(func() { requestCount.DeleteLabelValues(server, "trusted") })

	handled, m := askFrom(t, v, "user-db.shop.incus.", dns.TypeA, router)

	require.True(t, handled)
	require.Equal(t, dns.RcodeSuccess, m.Rcode)

	assert.Equal(t, float64(1), testutil.ToFloat64(requestCount.WithLabelValues(server, "trusted")))

	// The fail-closed counter is for queriers that could not be placed, and a
	// trusted one was never looked for.
	assert.Equal(t, float64(0), testutil.ToFloat64(deniedCount.WithLabelValues(server)))
}

// TestUnionForDeduplicatesAndOrders pins the helper on its own: the same record
// held by two views is one record, and the answer does not depend on map order.
func TestUnionForDeduplicatesAndOrders(t *testing.T) {
	t.Parallel()

	answer := ViewAnswer{
		apiView:  a("user-api.shop.incus.", "10.0.1.20"),
		userView: a("user-api.shop.incus.", "10.0.2.20"),
		bothView: a("user-api.shop.incus.", "10.0.1.20", "10.0.2.20"),
	}

	first := answer.UnionFor(dns.TypeA)
	require.Len(t, first, 2)

	got := make([]string, 0, len(first))
	for _, rr := range first {
		got = append(got, rr.(*dns.A).A.String())
	}

	assert.Equal(t, []string{"10.0.1.20", "10.0.2.20"}, got)

	// Same input, same order: a map walked at random would not promise this.
	for range 8 {
		again := answer.UnionFor(dns.TypeA)

		require.Len(t, again, len(first))

		for i := range first {
			assert.Equal(t, first[i].String(), again[i].String())
		}
	}

	assert.Empty(t, answer.UnionFor(dns.TypeMX))
	assert.Empty(t, ViewAnswer{}.UnionFor(dns.TypeA))
}
