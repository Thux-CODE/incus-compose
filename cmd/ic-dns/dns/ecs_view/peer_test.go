package ecs_view

import (
	"log/slog"
	"testing"

	"github.com/miekg/dns"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The peered example, one hub and two spokes peered with it and not with each
// other - the shape of a router's network and two project networks:
//
//	router    hub-net    10.0.9.0/24   peered with app-a and app-b
//	web       app-a-net  10.0.10.0/24  peered with hub
//	db        app-b-net  10.0.11.0/24  peered with hub
//
// A source writes a name under the views sitting on its network, and under the
// peer half of the views sitting on the networks it is peered with.
var (
	hubView = ViewOf([]string{"hub-net"})
	appA    = ViewOf([]string{"app-a-net"})
	appB    = ViewOf([]string{"app-b-net"})
)

// answerAddrs is an answer's A records as text, in answer order.
func answerAddrs(t *testing.T, m *dns.Msg) []string {
	t.Helper()

	out := make([]string, 0, len(m.Answer))

	for _, rr := range m.Answer {
		a, ok := rr.(*dns.A)
		require.True(t, ok, "an A query is answered with A records")

		out = append(out, a.A.String())
	}

	return out
}

func peeredPiece() *Snapshot {
	return newBuilder().
		zone("shop.incus.", false).
		name("router.shop.incus.", ViewAnswer{
			hubView:      a("router.shop.incus.", "10.0.9.2"),
			PeerOf(appA): a("router.shop.incus.", "10.0.9.2"),
			PeerOf(appB): a("router.shop.incus.", "10.0.9.2"),
		}).
		name("web.shop.incus.", ViewAnswer{
			appA:            a("web.shop.incus.", "10.0.10.2"),
			PeerOf(hubView): a("web.shop.incus.", "10.0.10.2"),
		}).
		name("db.shop.incus.", ViewAnswer{
			appB:            a("db.shop.incus.", "10.0.11.2"),
			PeerOf(hubView): a("db.shop.incus.", "10.0.11.2"),
		}).
		// A service with one replica on the hub and one behind a peer.
		name("api.shop.incus.", ViewAnswer{
			hubView:      a("api.shop.incus.", "10.0.9.3"),
			appA:         a("api.shop.incus.", "10.0.10.3"),
			PeerOf(appA): a("api.shop.incus.", "10.0.9.3"),
			// The hub sees app-a's replica through the peer, and its own directly.
			PeerOf(hubView): a("api.shop.incus.", "10.0.10.3"),
		}).
		at("10.0.9.2/32", hubView).
		at("10.0.10.2/32", appA).
		at("10.0.11.2/32", appB).
		at("10.0.9.0/24", hubView).
		at("10.0.10.0/24", appA).
		at("10.0.11.0/24", appB).
		done()
}

// peeredEngine serves the peered example, with the peer half on or off.
func peeredEngine(t *testing.T, on bool) *ECSView {
	t.Helper()

	v := New(slog.Default())
	v.PeerViews = on
	v.Replace(peeredPiece())
	v.SetHealthy(true)

	return v
}

// TestPeerHalfIsIgnoredWithoutTheFlag pins the default: whatever a source wrote
// under a peer half, an engine not told to read it answers as it always has.
func TestPeerHalfIsIgnoredWithoutTheFlag(t *testing.T) {
	t.Parallel()

	v := peeredEngine(t, false)

	for _, qname := range []string{"web.shop.incus.", "db.shop.incus."} {
		handled, m := ask(t, v, qname, dns.TypeA, "10.0.9.2")
		require.True(t, handled)
		assert.Equal(t, dns.RcodeNameError, m.Rcode, "%s from the hub without the flag", qname)
	}

	// Its own network answers exactly as before.
	handled, m := ask(t, v, "api.shop.incus.", dns.TypeA, "10.0.9.2")
	require.True(t, handled)
	assert.Equal(t, []string{"10.0.9.3"}, answerAddrs(t, m))
}

// TestPeerViewsAnswerOneHop is the matrix the flag exists for: the hub sees both
// spokes, each spoke sees the hub, and neither spoke sees the other.
func TestPeerViewsAnswerOneHop(t *testing.T) {
	t.Parallel()

	v := peeredEngine(t, true)

	cases := []struct {
		name  string
		qname string
		from  string
		want  []string
	}{
		{"the hub sees a spoke", "web.shop.incus.", "10.0.9.2", []string{"10.0.10.2"}},
		{"the hub sees the other spoke", "db.shop.incus.", "10.0.9.2", []string{"10.0.11.2"}},
		{"a spoke sees the hub", "router.shop.incus.", "10.0.10.2", []string{"10.0.9.2"}},
		{"a spoke still sees itself", "web.shop.incus.", "10.0.10.2", []string{"10.0.10.2"}},
		{"an unclaimed address on a spoke sees the hub", "router.shop.incus.", "10.0.10.77", []string{"10.0.9.2"}},
		{"own and peer replicas answer together", "api.shop.incus.", "10.0.9.2", []string{"10.0.9.3", "10.0.10.3"}},
		{"and from the other side too", "api.shop.incus.", "10.0.10.2", []string{"10.0.10.3", "10.0.9.3"}},
		{"not transitive: a spoke does not see the other spoke", "db.shop.incus.", "10.0.10.2", nil},
		{"not transitive, the other way round", "web.shop.incus.", "10.0.11.2", nil},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			handled, m := ask(t, v, c.qname, dns.TypeA, c.from)
			require.True(t, handled)

			if c.want == nil {
				assert.Equal(t, dns.RcodeNameError, m.Rcode)

				return
			}

			require.Equal(t, dns.RcodeSuccess, m.Rcode)
			assert.Equal(t, c.want, answerAddrs(t, m))
		})
	}
}

// TestPeerViewsLeaveDenialsAlone pins what does not move: a type nobody holds is
// NODATA through a peer as well, a name nobody holds is NXDOMAIN, and an alias
// claimed on both sides of a peering still reads as missing.
func TestPeerViewsLeaveDenialsAlone(t *testing.T) {
	t.Parallel()

	cname := func(name, target string) dns.RR {
		return &dns.CNAME{
			Hdr:    dns.RR_Header{Name: name, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: testTTL},
			Target: target,
		}
	}

	snap := newBuilder().
		zone("shop.incus.", false).
		name("web.shop.incus.", ViewAnswer{
			PeerOf(hubView): a("web.shop.incus.", "10.0.10.2"),
		}).
		name("www.shop.incus.", ViewAnswer{
			hubView:         Records{dns.TypeCNAME: []dns.RR{cname("www.shop.incus.", "router.shop.incus.")}},
			PeerOf(hubView): Records{dns.TypeCNAME: []dns.RR{cname("www.shop.incus.", "web.shop.incus.")}},
		}).
		at("10.0.9.0/24", hubView).
		done()

	v := New(slog.Default())
	v.PeerViews = true
	v.Replace(snap)
	v.SetHealthy(true)

	handled, m := ask(t, v, "web.shop.incus.", dns.TypeAAAA, "10.0.9.2")
	require.True(t, handled)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Empty(t, m.Answer, "NODATA through a peer")
	require.Len(t, m.Ns, 1)

	handled, m = ask(t, v, "nobody.shop.incus.", dns.TypeA, "10.0.9.2")
	require.True(t, handled)
	assert.Equal(t, dns.RcodeNameError, m.Rcode)

	handled, m = ask(t, v, "www.shop.incus.", dns.TypeA, "10.0.9.2")
	require.True(t, handled)
	assert.Equal(t, dns.RcodeNameError, m.Rcode, "two CNAMEs across the peering are refused like two on one network")

	handled, _ = ask(t, v, "elsewhere.example.", dns.TypeA, "10.0.9.2")
	assert.False(t, handled, "outside every zone still falls through")
}

// TestAnAddresslessNameIsNODATAThroughAPeer pins that a name held with no
// records - an instance up before its lease - reads the same through a peer as
// on its own network: NODATA, never NXDOMAIN.
func TestAnAddresslessNameIsNODATAThroughAPeer(t *testing.T) {
	t.Parallel()

	snap := newBuilder().
		zone("shop.incus.", false).
		name("web.shop.incus.", ViewAnswer{
			appA:            Records{},
			PeerOf(hubView): Records{},
		}).
		at("10.0.9.0/24", hubView).
		at("10.0.10.0/24", appA).
		done()

	v := New(slog.Default())
	v.PeerViews = true
	v.Replace(snap)
	v.SetHealthy(true)

	for _, from := range []string{"10.0.10.9", "10.0.9.9"} {
		handled, m := ask(t, v, "web.shop.incus.", dns.TypeA, from)
		require.True(t, handled)
		assert.Equal(t, dns.RcodeSuccess, m.Rcode, "from %s", from)
		assert.Empty(t, m.Answer, "from %s", from)
		require.Len(t, m.Ns, 1, "NODATA carries the SOA, from %s", from)
	}
}

// TestPeerAnswersAreCounted pins the result a peer answer publishes under: only
// an answer holding a record its own half did not is a peer answer.
func TestPeerAnswersAreCounted(t *testing.T) {
	// Not t.Parallel(): requestCount is package-level; the server name is this
	// test's own so nothing else moves these series.
	const server = "metrics-peer"

	v := peeredEngine(t, true)
	v.Metrics = true
	v.Server = server

	t.Cleanup(func() {
		requestCount.DeleteLabelValues(server, "peer")
		requestCount.DeleteLabelValues(server, "success")
	})

	_, m := ask(t, v, "web.shop.incus.", dns.TypeA, "10.0.9.2")
	require.Equal(t, dns.RcodeSuccess, m.Rcode)

	_, m = ask(t, v, "api.shop.incus.", dns.TypeA, "10.0.9.2")
	require.Equal(t, dns.RcodeSuccess, m.Rcode)

	// Own network only: the peer half holds nothing for this name.
	_, m = ask(t, v, "web.shop.incus.", dns.TypeA, "10.0.10.2")
	require.Equal(t, dns.RcodeSuccess, m.Rcode)

	assert.Equal(t, float64(2), testutil.ToFloat64(requestCount.WithLabelValues(server, "peer")))
	assert.Equal(t, float64(1), testutil.ToFloat64(requestCount.WithLabelValues(server, "success")))
}

// TestPeerOfNamesNoNetworks pins that a peer half cannot collide with a view: a
// set of networks never starts with the byte it does.
func TestPeerOfNamesNoNetworks(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, hubView, PeerOf(hubView))
	assert.NotEqual(t, PeerOf(appA), PeerOf(appB))
	assert.NotEqual(t, AmbiguousView, PeerOf(""))
}
