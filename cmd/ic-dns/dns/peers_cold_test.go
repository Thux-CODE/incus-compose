package dns

import (
	"errors"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// answersWithin asks one name from one querier until it answers want, or fails
// the test after a few seconds.
func answersWithin(t *testing.T, r *running, qname, from string, want int, why string) {
	t.Helper()

	a := &adapter{chain: r.p.view}
	deadline := time.Now().Add(3 * time.Second)

	for {
		rcode, _ := resolve(a, qname, dns.TypeA, from)
		if rcode == want {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("%s: %s from %s answered rcode %d, want %d", why, qname, from, rcode, want)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// hubFleetEvents is the hub fleet as the enricher hands it over, restored or
// read in this run.
func hubFleetEvents(restored bool) []*iutil.Event {
	var out []*iutil.Event

	for _, inst := range hubFleet() {
		ev := inst.Event
		if restored {
			ev = ev.WithRestored()
		}

		out = append(out, ev)
	}

	return out
}

// TestARestartAnswersThroughThePeeringItStored is the restart the cold store
// exists for, with peer views on: the peering a run last held is stored with
// the serials, and the next run replays its fleet under it, so the names a
// querier sees only through a peer answer from the store at once - while the
// peering itself cannot be read yet. The read, once it lands, corrects it.
func TestARestartAnswersThroughThePeeringItStored(t *testing.T) {
	const (
		mgmt = "10.146.11.6"
		web  = "10.147.250.2"
	)

	// A first run that read the peering and the fleet.
	first := runPeered(t, hubIncus(), Suffix("incus"), TTL(30), ColdMemory(nil))
	first.feed(t, append(hubFleetEvents(false), event(iutil.ActionSweepEnd, "", ""))...)
	answersWithin(t, first, "django-1.pvlab2.incus.", mgmt, dns.RcodeSuccess, "first run")
	first.stop(t)

	_, stored, err := decodeCold(first.p.cold.held)
	require.NoError(t, err)
	assert.Equal(t, hubPeers(), stored, "the peering goes to the store with the serials")

	// The next run, against an Incus that does not answer the peering yet.
	f := hubIncus()
	f.down = errors.New("incusd is not answering")

	next := runPeered(t, f, Suffix("incus"), TTL(30), ColdMemory(first.p.cold.held))
	next.feed(t, hubFleetEvents(true)...)

	answersWithin(t, next, "django-1.pvlab2.incus.", mgmt, dns.RcodeSuccess, "hub through a peer, from the store")
	answersWithin(t, next, "django.pvlab1.incus.", mgmt, dns.RcodeSuccess, "a service through a peer, from the store")
	answersWithin(t, next, "mgmt.default.incus.", web, dns.RcodeSuccess, "spoke through a peer, from the store")
	answersWithin(t, next, "django-1.pvlab2.incus.", web, dns.RcodeServerFailure, "still not transitive, and cold")

	// Incus answers again, and pvlab2 was unpeered while this was down.
	f.mu.Lock()
	f.down = nil
	f.peers[hubNet] = f.peers[hubNet][:1]
	delete(f.peers, app2)
	f.mu.Unlock()

	answersWithin(t, next, "django-1.pvlab2.incus.", mgmt, dns.RcodeServerFailure, "the read takes the stale peer half out")
	answersWithin(t, next, "django-1.pvlab1.incus.", mgmt, dns.RcodeSuccess, "and keeps the one still peered")

	next.feed(t, append(hubFleetEvents(false), warm(event(iutil.ActionSweepEnd, "", "")))...)

	answersWithin(t, next, "django-1.pvlab2.incus.", mgmt, dns.RcodeNameError, "warm: refused for good")

	next.stop(t)

	_, stored, err = decodeCold(next.p.cold.held)
	require.NoError(t, err)
	assert.Equal(t, onlyApp1(), stored, "and the corrected peering is what is stored")
}

// TestThePeeringIsStoredOnlyWhereItIsFollowed pins the flag off: nothing about
// a peering is stored, and a stored one is not restored.
func TestThePeeringIsStoredOnlyWhereItIsFollowed(t *testing.T) {
	seed, err := encodeCold(map[string]zoneSerial{"pvlab1.incus.": {Serial: 3}}, hubPeers())
	require.NoError(t, err)

	r := start(t, Suffix("incus"), TTL(30), ColdMemory(seed))
	r.feed(t, append(hubFleetEvents(true), event(iutil.ActionSweepEnd, "", ""))...)

	answersWithin(t, r, "mgmt.default.incus.", "10.147.250.2", dns.RcodeNameError, "no peer half without the flag")

	r.stop(t)

	assert.Empty(t, r.p.state.peers)

	_, stored, err := decodeCold(r.p.cold.held)
	require.NoError(t, err)
	assert.Nil(t, stored)
}

// TestAColdStoreWithoutPeeringLoads pins the format: a store written before the
// peering was stored, or by a build that does not store it, loads with none.
func TestAColdStoreWithoutPeeringLoads(t *testing.T) {
	t.Parallel()

	serials, peers, err := decodeCold([]byte(`{"version":1,"serials":{"shop.incus.":{"Serial":7}}}`))
	require.NoError(t, err)
	assert.Equal(t, uint32(7), serials["shop.incus."].Serial)
	assert.Nil(t, peers)

	b, err := encodeCold(serials, nil)
	require.NoError(t, err)
	assert.NotContains(t, string(b), "peers", "no peering is no key, as before")
}
