package dns

import (
	"net/netip"
	"testing"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

func TestFourthLegacyFirstPublicationIsComplete(t *testing.T) {
	const zone = "shop.example."
	seed := []byte(`{"version":1,"serials":{"shop.example.":{"Serial":42}}}`)
	serials, _, err := decodeCold(seed)
	require.NoError(t, err)
	c := newColdServer(t, Suffix("example"), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	c.p.state = newState(serials)
	web := func(ip string) *iutil.Event {
		return eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": ip}).WithProject(iutil.NewProject(optedIn()))
	}
	c.p.fold(web("10.0.0.2").WithRestored())
	c.p.fold(web("10.0.0.9"))
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	peer := xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, peer, xfrQuery(zone, 42)))
	require.Greater(t, len(peer.records()), 1, "secondary got %v", peer.records())
	require.Equal(t, uint32(43), c.p.state.serials[zone].Serial)
}

func TestFourthInterruptedLegacyReplay(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"), ColdMemory(nil))
	c.p.state = newState(map[string]zoneSerial{zone: {Serial: 42}})
	web := eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": "10.0.0.2"}).WithRestored()
	db := eventOn(api.EventLifecycleInstanceUpdated, "shop", "db", nil, map[string]string{"a": "10.0.0.3"}).WithRestored()
	c.p.fold(web)
	c.p.write()
	serials, _, err := decodeCold(<-c.p.cold.writes)
	require.NoError(t, err)
	next := newColdServer(t, Suffix("example"))
	next.p.state = newState(serials)
	next.p.fold(web)
	next.p.fold(db)
	next.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	require.Equal(t, uint32(42), next.p.state.serials[zone].Serial)
}

func TestFourthUnchangedConfirmationBeforeLegacyWindow(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"), ColdMemory(nil))
	c.p.state = newState(map[string]zoneSerial{zone: {Serial: 42}})
	web := eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": "10.0.0.2"})
	c.p.fold(web.WithRestored())
	c.p.fold(web)
	c.p.write()
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	require.Equal(t, uint32(42), c.p.state.serials[zone].Serial, "the two events describe identical records")
}

func TestFourthLegacyDebtAndModernCheckpoint(t *testing.T) {
	const zone = "shop.example."
	serials, _, err := decodeCold([]byte(`{"version":1,"serials":{"shop.example.":{"Serial":42,"owed":true}}}`))
	require.NoError(t, err)
	web := eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": "10.0.0.2"}).WithRestored()
	for iteration := range 3 {
		c := newColdServer(t, Suffix("example"), ColdMemory(nil))
		c.p.state = newState(serials)
		c.p.fold(web)
		if iteration != 0 {
			c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
			require.Equal(t, uint32(43), c.p.state.serials[zone].Serial)
		} else {
			c.p.write()
		}
		serials, _, err = decodeCold(<-c.p.cold.writes)
		require.NoError(t, err)
		if iteration == 0 {
			require.True(t, serials[zone].Owed)
		}
	}
}

// A restart halfway through the replay of a zone a read had already changed
// keeps the step it owes: the cold store written meanwhile says owed, and
// the full replay after it still steps once.
func TestFourthInterruptedLegacyReplayKeepsWhatIsOwed(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"), ColdMemory(nil))
	c.p.state = newState(map[string]zoneSerial{zone: {Serial: 42}})
	web := eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": "10.0.0.2"}).WithRestored()
	c.p.fold(web)
	c.p.write()
	<-c.p.cold.writes
	c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": "10.0.0.9"}))
	c.p.write()
	serials, _, err := decodeCold(<-c.p.cold.writes)
	require.NoError(t, err)
	require.True(t, serials[zone].Owed)
	require.Empty(t, serials[zone].Digest)
	next := newColdServer(t, Suffix("example"))
	next.p.state = newState(serials)
	next.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": "10.0.0.9"}).WithRestored())
	next.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	require.Equal(t, uint32(43), next.p.state.serials[zone].Serial)
}

// A read of the new run that differs from the replay only in a config key
// ic-dns does not read changes nothing a legacy serial answers for.
func TestFourthLegacyReadWithForeignConfigKey(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"))
	c.p.state = newState(map[string]zoneSerial{zone: {Serial: 42}})
	web := func(note string) *iutil.Event {
		return event(api.EventLifecycleInstanceUpdated, "shop", "web").
			WithInstance(read("shop", true, map[string]string{"user.a2lab.note": note}, netsOf(map[string]string{"a": "10.0.0.2"})), true)
	}
	c.p.fold(web("a").WithRestored())
	c.p.write()
	c.p.fold(web("b"))
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	require.Equal(t, uint32(42), c.p.state.serials[zone].Serial)
}

// A delete of an instance the replay held is a change the legacy serial did
// not describe: the zone steps once, in whatever window the delete landed.
func TestFourthLegacyDeleteOfReplayedInstance(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"))
	c.p.state = newState(map[string]zoneSerial{zone: {Serial: 42}})
	for name, ip := range map[string]string{"web": "10.0.0.2", "db": "10.0.0.3"} {
		c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "shop", name, nil, map[string]string{"a": ip}).WithRestored())
	}
	c.p.fold(event(api.EventLifecycleInstanceDeleted, "shop", "db"))
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	require.Equal(t, uint32(43), c.p.state.serials[zone].Serial)
}

func TestFourthChildKeepsItsLastHostAfterParentAliasGoes(t *testing.T) {
	const child = "api.shop.example."
	c := newColdServer(t, Suffix("example"), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	parent := eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", map[string]string{metaAliases: "v1.api"}, map[string]string{"a": "10.0.0.2"})
	c.p.fold(parent)
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	worker := eventOn(api.EventLifecycleInstanceUpdated, "other", "worker", map[string]string{metaZone: child}, map[string]string{"a": "10.0.0.3"})
	c.p.fold(worker.WithProject(iutil.NewProject(optedIn())))
	c.p.write()
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, xfrPeer(true), xfrQuery(child, 0)))
	c.p.fold(event(api.EventLifecycleInstanceDeleted, "shop", "web"))
	c.p.write()
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, xfrPeer(true), xfrQuery(child, 0)), "zone %s still has worker", child)
}

// The parent keeps the count of the alias it got while the child carries it,
// and the child going gives it back: the alias then leaves the parent, which
// it never left, and nothing is counted twice or below zero.
func TestFourthParentAliasCountedOnceAcrossChild(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"))
	c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", map[string]string{metaAliases: "v1.api"}, map[string]string{"a": "10.0.0.2"}))
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	names := c.p.state.serials[zone].names
	c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "other", "worker", map[string]string{metaZone: "api." + zone}, map[string]string{"a": "10.0.0.3"}))
	c.p.write()
	c.p.fold(event(api.EventLifecycleInstanceDeleted, "other", "worker"))
	c.p.write()
	require.Equal(t, names, c.p.state.serials[zone].names)
	require.Zero(t, c.p.state.serials["api."+zone].names)
	c.p.fold(event(api.EventLifecycleInstanceDeleted, "shop", "web"))
	c.p.write()
	require.Zero(t, c.p.state.serials[zone].names)
}
