package dns

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// peersAsked reports whether f has been asked for one network's peers.
func peersAsked(f *fakePeers, key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Contains(f.asked, key)
}

// TestANetworkGettingItsFirstNameIsRead pins the reads by network: only the
// networks a name is on are read, the first name on another one asks for a
// read of it, and a round does not turn warm until a read covering it lands -
// a read made before it does not count.
func TestANetworkGettingItsFirstNameIsRead(t *testing.T) {
	const mgmt = "10.146.11.6"

	f := hubIncus()
	f.fail[app2] = errors.New("connection reset")

	r := runPeered(t, f, Suffix("incus"), TTL(30))

	held := hubFleet()
	r.feed(t, held["default/mgmt"].Event, held["pvlab1/django-1"].Event)

	require.Eventually(t, func() bool { return peersAsked(f, app1) }, 2*time.Second, 5*time.Millisecond)
	answersWithin(t, r, "django-1.pvlab1.incus.", mgmt, dns.RcodeSuccess, "the networks held are read")
	assert.False(t, peersAsked(f, "default/incus-uplink"), "nothing nobody sits on is read")

	// The first name on pvlab2/app: its peers are asked for, and fail for now.
	r.feed(t, held["pvlab2/django-1"].Event, event(iutil.ActionSweepEnd, "", ""))

	require.Eventually(t, func() bool { return peersAsked(f, app2) }, 2*time.Second, 5*time.Millisecond)
	answersWithin(t, r, "nobody.default.incus.", mgmt, dns.RcodeServerFailure, "not warm without pvlab2/app read")
	assert.False(t, r.p.view.Ready())
	assert.Error(t, r.p.PeeringReady())

	f.mu.Lock()
	delete(f.fail, app2)
	f.mu.Unlock()

	answersWithin(t, r, "django-1.pvlab2.incus.", mgmt, dns.RcodeSuccess, "read once it answers")
	answersWithin(t, r, "nobody.default.incus.", mgmt, dns.RcodeNameError, "and warm")
	assert.NoError(t, r.p.PeeringReady())

	r.stop(t)
}

// TestPeerTargetsAreHeldNetworksAndThePeering pins what a read covers: every
// network a name is on, and every one the held peering names, so a read made
// before a replay lands still reads what a restored peering holds.
func TestPeerTargetsAreHeldNetworksAndThePeering(t *testing.T) {
	t.Parallel()

	s := newState(nil)
	assert.Empty(t, s.peerTargets())

	s.setPeers(onlyApp1(), 5)
	assert.Equal(t, []string{hubNet, app1}, s.peerTargets())

	held := hubFleet()
	s.apply("pvlab2/django-1", held["pvlab2/django-1"], 5)
	assert.Equal(t, []string{app2}, s.grown, "a name on a network nothing was on")
	assert.Equal(t, []string{hubNet, app1, app2}, s.peerTargets())
}
