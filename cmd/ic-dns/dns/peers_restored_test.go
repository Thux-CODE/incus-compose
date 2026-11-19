package dns

import (
	"errors"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// TestARestoredPeeringDoesNotWaitOnARead pins the restart on a member too slow
// to read the peering: the peering the cold store kept counts as known, so the
// round turns the view warm and /ready ready while every read still fails, and
// names through a peer answer warm from it. The read, once it lands, corrects
// it as usual.
func TestARestoredPeeringDoesNotWaitOnARead(t *testing.T) {
	const mgmt = "10.146.11.6"

	seed, err := encodeCold(map[string]zoneSerial{}, hubPeers())
	require.NoError(t, err)

	f := hubIncus()
	f.down = errors.New("context deadline exceeded")

	r := runPeered(t, f, Suffix("incus"), TTL(30), ColdMemory(seed))
	r.feed(t, append(hubFleetEvents(true), event(iutil.ActionSweepEnd, "", ""))...)

	answersWithin(t, r, "nobody.default.incus.", mgmt, dns.RcodeNameError, "warm at the round, no read needed")
	assert.True(t, r.p.view.Ready())
	assert.NoError(t, r.p.PeeringReady())

	rcode, got := resolve(&adapter{chain: r.p.view}, "django-1.pvlab2.incus.", dns.TypeA, mgmt)
	assert.Equal(t, dns.RcodeSuccess, rcode, "through the restored peering")
	assert.Equal(t, []string{"10.147.251.2"}, got)

	// pvlab2 was unpeered while this was down; the read lands at last.
	f.mu.Lock()
	f.down = nil
	f.peers[hubNet] = f.peers[hubNet][:1]
	delete(f.peers, app2)
	f.mu.Unlock()

	answersWithin(t, r, "django-1.pvlab2.incus.", mgmt, dns.RcodeNameError, "the read corrects it")
	answersWithin(t, r, "django-1.pvlab1.incus.", mgmt, dns.RcodeSuccess, "and keeps what is still peered")

	r.stop(t)
}

// TestNoRestoredPeeringStillWaits pins the other start: nothing in the cold
// store, nothing to answer through a peer from, so the round waits on a read.
func TestNoRestoredPeeringStillWaits(t *testing.T) {
	f := hubIncus()
	f.down = errors.New("context deadline exceeded")

	r := runPeered(t, f, Suffix("incus"), TTL(30), ColdMemory(nil))
	r.feed(t, append(hubFleetEvents(true), event(iutil.ActionSweepEnd, "", ""))...)

	answersWithin(t, r, "nobody.default.incus.", "10.146.11.6", dns.RcodeServerFailure, "cold without a peering")
	assert.Error(t, r.p.PeeringReady())

	r.stop(t)
}
