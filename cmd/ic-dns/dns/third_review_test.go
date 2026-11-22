package dns

import (
	"net/netip"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

func TestThirdLegacyReplayInTwoPublications(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"))
	c.p.state = newState(map[string]zoneSerial{zone: {Serial: 42}})
	for name, ip := range map[string]string{"web": "10.0.0.2", "db": "10.0.0.3"} {
		c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "shop", name, nil, map[string]string{"a": ip}).WithRestored())
		c.p.write()
	}
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	require.Equal(t, uint32(42), c.p.state.serials[zone].Serial)
}

// The digest taken again at each window of the replay stops at the first
// change that is not the replay: what this run read differently from what the
// legacy serial described still steps it, once.
func TestThirdLegacyReplayThenReadChangesBeforeRound(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"))
	c.p.state = newState(map[string]zoneSerial{zone: {Serial: 42}})
	for name, ip := range map[string]string{"web": "10.0.0.2", "db": "10.0.0.3"} {
		c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "shop", name, nil, map[string]string{"a": ip}).WithRestored())
		c.p.write()
	}
	c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": "10.0.0.9"}))
	c.p.write()
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	require.Equal(t, uint32(43), c.p.state.serials[zone].Serial)
}

// A zone of a legacy file the replay published nothing of before a read of
// this run reached it has no digest of what its serial described: it steps.
func TestThirdLegacyZoneReadBeforeAnyReplay(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"))
	c.p.state = newState(map[string]zoneSerial{zone: {Serial: 42}})
	c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": "10.0.0.2"}))
	c.p.write()
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	require.Equal(t, uint32(43), c.p.state.serials[zone].Serial)
}

func TestThirdParentTransferAfterChildZoneAppears(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	parent := eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", map[string]string{metaAliases: "v1.api"}, map[string]string{"a": "10.0.0.2"})
	c.p.fold(parent.WithProject(iutil.NewProject(optedIn())))
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	transfer := func(serial uint32) []dns.RR {
		peer := xfrPeer(true)
		require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, peer, xfrQuery(zone, serial)))
		return peer.records()
	}
	before := transfer(0)
	c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "other", "worker", map[string]string{metaZone: "api." + zone}, map[string]string{"a": "10.0.0.3"}))
	c.p.write()
	after := transfer(0)
	alias := func(rr dns.RR) bool { return rr.Header().Name == "v1.api."+zone }
	require.True(t, slices.ContainsFunc(before, alias), "%v", before)
	require.False(t, slices.ContainsFunc(after, alias), "%v", after)
	require.Greater(t, len(transfer(before[0].(*dns.SOA).Serial)), 1)
}

// The other way round: the child zone going gives the alias back to the
// parent, whose serial steps with it.
func TestThirdParentTransferAfterChildZoneGoes(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	parent := eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", map[string]string{metaAliases: "v1.api"}, map[string]string{"a": "10.0.0.2"})
	c.p.fold(parent.WithProject(iutil.NewProject(optedIn())))
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "other", "worker", map[string]string{metaZone: "api." + zone}, map[string]string{"a": "10.0.0.3"}))
	c.p.write()
	transfer := func(serial uint32) []dns.RR {
		peer := xfrPeer(true)
		require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, peer, xfrQuery(zone, serial)))
		return peer.records()
	}
	before := transfer(0)
	c.p.fold(event(api.EventLifecycleInstanceDeleted, "other", "worker"))
	c.p.write()
	after := transfer(0)
	alias := func(rr dns.RR) bool { return rr.Header().Name == "v1.api."+zone }
	require.False(t, slices.ContainsFunc(before, alias), "%v", before)
	require.True(t, slices.ContainsFunc(after, alias), "%v", after)
	require.Greater(t, len(transfer(before[0].(*dns.SOA).Serial)), 1)
}

func TestThirdLatestConfirmedPeerTableIsStored(t *testing.T) {
	c := newColdServer(t, Suffix("incus"), PeerViews(true), ColdMemory(nil))
	for _, ev := range hubFleetEvents(false) {
		c.p.fold(ev)
	}
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	first := <-c.p.peerAsk
	c.p.peered(peerRead{keys: first.keys, gen: first.gen, peers: map[string][]string{}})
	c.p.askPeers(false)
	old := <-c.p.peerAsk
	c.p.fold(event(api.EventLifecycleNetworkPeerCreated, "default", "hub"))
	fresh := <-c.p.peerAsk
	c.p.peered(peerRead{keys: old.keys, gen: old.gen, peers: hubPeers()})
	c.p.peered(peerRead{keys: fresh.keys, gen: fresh.gen, peers: hubPeers()})
	_, stored, err := decodeCold(<-c.p.cold.writes)
	require.NoError(t, err)
	require.Equal(t, hubPeers(), stored)
}

func TestThirdSettledPeerDebtReachesPublishedSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newColdServer(t, Suffix("incus"), PeerViews(true))
		c.p.peerReader = hubIncus()
		f := hubFleet()
		empty := map[string][]string{}
		c.p.fold(f["default/mgmt"].Event)
		c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
		first := <-c.p.peerAsk
		c.p.peered(peerRead{keys: first.keys, gen: first.gen, peers: empty})
		c.p.fold(f["pvlab1/django-1"].Event)
		c.p.write()
		one := <-c.p.peerAsk
		time.Sleep(30 * time.Second)
		c.p.fold(f["pvlab2/django-1"].Event)
		c.p.write()
		c.p.peered(peerRead{keys: one.keys, gen: one.gen, peers: empty})
		time.Sleep(16 * time.Second)
		require.NotContains(t, c.p.peerOwed, app1)
		code, _ := resolve(c.a, "django-1.pvlab1.incus.", dns.TypeA, "10.146.11.6")
		require.Equal(t, "NXDOMAIN", dns.RcodeToString[code])
	})
}
