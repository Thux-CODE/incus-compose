package ecs_view

import (
	"log/slog"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPeerAnswersBeforeWarm pins the cold rules through a peer: a name held in
// the peer half is answered from it, clamped, as on the querier's own network;
// what a peer half refuses - an alias claimed on both sides - is SERVFAIL until
// warm, since the claimant that has to go may only not have been read yet, and
// NXDOMAIN after. With --cold-answers off a hit through a peer fails too.
func TestPeerAnswersBeforeWarm(t *testing.T) {
	t.Parallel()

	cname := func(name, target string) dns.RR {
		return &dns.CNAME{
			Hdr:    dns.RR_Header{Name: name, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: testTTL},
			Target: target,
		}
	}

	snap := func() *Snapshot {
		return newBuilder().
			zone("shop.incus.", false).
			name("web.shop.incus.", ViewAnswer{
				appA:            a("web.shop.incus.", "10.0.10.2"),
				PeerOf(hubView): a("web.shop.incus.", "10.0.10.2"),
			}).
			name("www.shop.incus.", ViewAnswer{
				hubView:         Records{dns.TypeCNAME: []dns.RR{cname("www.shop.incus.", "router.shop.incus.")}},
				PeerOf(hubView): Records{dns.TypeCNAME: []dns.RR{cname("www.shop.incus.", "web.shop.incus.")}},
			}).
			at("10.0.9.0/24", hubView).
			at("10.0.10.0/24", appA).
			done()
	}

	v := New(slog.Default())
	v.PeerViews = true
	v.Claims = []string{"incus."}
	v.Replace(snap())

	handled, m := ask(t, v, "web.shop.incus.", dns.TypeA, "10.0.9.2")
	require.True(t, handled)
	require.Equal(t, dns.RcodeSuccess, m.Rcode, "a peer hit is answered before warm")
	assert.Equal(t, []string{"10.0.10.2"}, answerAddrs(t, m))
	assert.EqualValues(t, staleTTL, m.Answer[0].Header().Ttl, "clamped like any cold answer")

	handled, m = ask(t, v, "www.shop.incus.", dns.TypeA, "10.0.9.2")
	require.True(t, handled)
	assert.Equal(t, dns.RcodeServerFailure, m.Rcode, "an alias claimed twice through a peer is a cold miss")

	v.SetHealthy(true)

	_, m = ask(t, v, "www.shop.incus.", dns.TypeA, "10.0.9.2")
	assert.Equal(t, dns.RcodeNameError, m.Rcode, "and refused once warm")

	// Asked not to answer from the store: a peer hit fails like an own one.
	off := New(slog.Default())
	off.PeerViews = true
	off.ColdAnswers = false
	off.Claims = []string{"incus."}
	off.Replace(snap())

	_, m = ask(t, off, "web.shop.incus.", dns.TypeA, "10.0.9.2")
	assert.Equal(t, dns.RcodeServerFailure, m.Rcode)
}
