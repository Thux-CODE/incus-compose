package ecs_view

import (
	"net/netip"
	"testing"

	"github.com/miekg/dns"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTrustedIsIndependentOfPeerViews pins that the two never stack: a trusted
// querier gets the union of everything, peer halves included and deduplicated,
// and is counted as trusted, not as peer.
func TestTrustedIsIndependentOfPeerViews(t *testing.T) {
	// Not t.Parallel(): requestCount is package-level; the server name is this
	// test's own so nothing else moves these series.
	const server = "metrics-trusted-peer"

	v := peeredEngine(t, true)
	v.Trusted = []netip.Prefix{netip.MustParsePrefix("10.0.9.2/32")}
	v.Metrics = true
	v.Server = server

	t.Cleanup(func() {
		requestCount.DeleteLabelValues(server, "trusted")
		requestCount.DeleteLabelValues(server, "peer")
	})

	// From the hub, trusted: the spokes too, and the service's two replicas once each.
	handled, m := askFrom(t, v, "api.shop.incus.", dns.TypeA, "10.0.9.2")
	require.True(t, handled)
	assert.ElementsMatch(t, []string{"10.0.9.3", "10.0.10.3"}, answerAddrs(t, m))

	// A spoke the hub sees through its peering is still a trusted answer: the
	// exemption is checked first and the peer half is never asked.
	handled, m = askFrom(t, v, "db.shop.incus.", dns.TypeA, "10.0.9.2")
	require.True(t, handled)
	assert.Equal(t, []string{"10.0.11.2"}, answerAddrs(t, m))

	assert.Equal(t, float64(2), testutil.ToFloat64(requestCount.WithLabelValues(server, "trusted")))
	assert.Equal(t, float64(0), testutil.ToFloat64(requestCount.WithLabelValues(server, "peer")))

	// A spoke, not trusted, is answered by its peering alone, whatever the
	// trusted list says about somebody else.
	handled, m = askFrom(t, v, "db.shop.incus.", dns.TypeA, "10.0.10.2")
	require.True(t, handled)
	assert.Equal(t, dns.RcodeNameError, m.Rcode)
}
