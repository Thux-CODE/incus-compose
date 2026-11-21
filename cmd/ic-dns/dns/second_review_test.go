package dns

import (
	"fmt"
	"log/slog"
	"net/netip"
	"runtime"
	"testing"
	"time"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/shared"
)

// published is what a previous run left in its cold store for zone, having
// folded feed whole: serial, and the digest of what the zone held then - the
// premise "the disk describes this at serial N" as this build writes it.
func published(t *testing.T, zone string, serial uint32, opts []Option, feed ...*iutil.Event) map[string]zoneSerial {
	t.Helper()

	p := New(slog.Default(), opts...)
	p.next = func(*iutil.Event) {}

	for _, ev := range feed {
		p.fold(ev)
	}

	p.fold(warm(event(iutil.ActionSweepEnd, "", "")))

	digest := p.state.serials[zone].Digest
	require.NotEmpty(t, digest)

	return map[string]zoneSerial{zone: {Serial: serial, Digest: digest}}
}

// TestSecondDeleteInsideReplayWindow: the disk holds one and two at 42; the
// replay of two and its live delete fall in one window. What the zone carries
// at the end of the round is not what 42 was published with, so it steps.
//
// Adapted: the seed carries the digest this build stores with 42, which is how
// a disk "holds one and two at 42" now.
func TestSecondDeleteInsideReplayWindow(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	one := eventOn(api.EventLifecycleInstanceUpdated, "shop", "one", nil, map[string]string{"a": "10.0.0.1"}).WithProject(iutil.NewProject(optedIn()))
	two := eventOn(api.EventLifecycleInstanceUpdated, "shop", "two", nil, map[string]string{"a": "10.0.0.2"}).WithProject(iutil.NewProject(optedIn()))
	c.p.state = newState(published(t, zone, 42, []Option{Suffix("example"), TTL(30)}, one, two))
	c.p.fold(one.WithRestored())
	c.p.write()
	c.p.fold(two.WithRestored())
	c.p.fold(event(api.EventLifecycleInstanceDeleted, "shop", "two"))
	c.p.fold(one)
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	full := xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, full, xfrQuery(zone, 0)))
	var names []string
	for _, rr := range full.records() {
		if a, ok := rr.(*dns.A); ok {
			names = append(names, a.Hdr.Name)
		}
	}
	require.Equal(t, []string{"one." + zone}, names)
	ix := xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, ix, xfrQuery(zone, 42)))
	require.Greater(t, len(ix.records()), 1, "the secondary still holds two names at 42")
}

// TestSecondPeerChangeAllReplicasRestored: two networks offered two service
// addresses at 42; during the replay they become peers and the healthy
// replica takes the starting one out of the zone too. The zone changed, and
// steps, though every replica is still the restored one.
//
// Adapted as above: the seed is what 42 was published with, both addresses.
func TestSecondPeerChangeAllReplicasRestored(t *testing.T) {
	const zone = "shop.example."
	var replicas []*iutil.Event
	for i, status := range []string{shared.HealthStatusHealthy, shared.HealthStatusStarting} {
		network := []string{"a", "b"}[i]
		ip := []string{"10.0.0.2", "10.0.1.2"}[i]
		cfg := map[string]string{userLabel(labelServiceCompose): "api", shared.HealthEnabledKey: "true",
			shared.HealthKeyPrefix + "test": healthTest, shared.HealthStatusKey: status}
		ev := event(api.EventLifecycleInstanceUpdated, "shop", network).WithInstance(read("shop", true, cfg, netsOf(map[string]string{network: ip})), true)
		ev = ev.WithProject(iutil.NewProject(optedIn()))
		replicas = append(replicas, ev)
	}
	c := newColdServer(t, Suffix("example"), PeerViews(true), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	c.p.state = newState(published(t, zone, 42, []Option{Suffix("example"), TTL(30)}, replicas...))
	for _, ev := range replicas {
		c.p.fold(ev.WithRestored())
	}
	c.p.write()
	service := func() []string {
		snap := c.p.xfr.current.Load()
		z := snap.ZoneOf(zone)
		var ips []string
		for _, rr := range zoneRecords(snap.Tree.Root(), snap.Denial.Root(), z, z.SOA) {
			if a, ok := rr.(*dns.A); ok && a.Hdr.Name == "api."+zone {
				ips = append(ips, a.A.String())
			}
		}
		return ips
	}
	require.ElementsMatch(t, []string{"10.0.0.2", "10.0.1.2"}, service())
	ask := <-c.p.peerAsk
	c.p.peered(peerRead{gen: ask.gen, keys: ask.keys, peers: map[string][]string{onWire("a"): {onWire("b")}, onWire("b"): {onWire("a")}}})
	for _, ev := range replicas {
		c.p.fold(ev)
	}
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	require.Equal(t, []string{"10.0.0.2"}, service())
	ix := xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, ix, xfrQuery(zone, 42)))
	require.Greater(t, len(ix.records()), 1, "a service address disappeared from the zone at serial 42")
}

// TestSecondIndependentStoreCheckpoints: the enricher stored the new address,
// and ic-dns went down with it still in its publish window, its store saying
// the old address at the serial a secondary holds. After the restart the
// replay is the new address, confirmed live: what the zone carries is not what
// that serial was published with, and it steps.
func TestSecondIndependentStoreCheckpoints(t *testing.T) {
	const zone = "shop.example."
	allow := AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")})
	record := func(ip string) *iutil.Event {
		return eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": ip}).WithProject(iutil.NewProject(optedIn()))
	}
	a := newColdServer(t, Suffix("example"), allow, ColdMemory(nil))
	a.p.fold(record("10.0.0.1"))
	a.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	old := xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, a.p.xfr, old, xfrQuery(zone, 0)))
	serial := old.records()[0].(*dns.SOA).Serial
	current := record("10.0.0.2")
	a.p.fold(current)
	// The enricher saved the new read after handing it to DNS. DNS still has
	// it in its 150 ms window when the process exits without a drain.
	b := newColdServer(t, Suffix("example"), allow, ColdMemory(<-a.p.cold.writes))
	serials, _ := b.p.cold.load()
	b.p.state = newState(serials)
	b.p.fold(current.WithRestored())
	b.p.write()
	b.p.fold(current)
	b.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	_, ips := resolve(b.a, "web."+zone, dns.TypeA, "10.0.0.2")
	require.Equal(t, []string{"10.0.0.2"}, ips)
	ix := xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, b.p.xfr, ix, xfrQuery(zone, serial)))
	require.Greater(t, len(ix.records()), 1, "the secondary still has 10.0.0.1")
}

// TestSecondZoneRecreatedBetweenPolls: the last instance leaves, the zone
// goes for one publish and comes back with another address. Its serial goes
// on from where it was, not from 1, which the secondary may hold.
func TestSecondZoneRecreatedBetweenPolls(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	create := func(ip string) {
		c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": ip}).WithProject(iutil.NewProject(optedIn())))
	}
	create("10.0.0.1")
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	before := c.p.state.serials[zone].Serial
	c.p.fold(event(api.EventLifecycleInstanceDeleted, "shop", "web"))
	c.p.write()
	create("10.0.0.2")
	c.p.write()
	ix := xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, ix, xfrQuery(zone, before)))
	require.Greater(t, len(ix.records()), 1, "secondary polling across zone recreation still has the old address")
}

// TestSecondColdIXFRAndReplayConfirmation: the replay and an identical live
// confirmation fall before the first publish. The zone is what 42 was
// published with, so it stays 42: a secondary holding it transfers nothing.
//
// Adapted as above: the seed carries the digest of web at 10.0.0.2.
func TestSecondColdIXFRAndReplayConfirmation(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	ev := eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": "10.0.0.2"}).WithProject(iutil.NewProject(optedIn()))
	c.p.state = newState(published(t, zone, 42, []Option{Suffix("example"), TTL(30)}, ev))
	c.p.fold(ev.WithRestored())
	ix := xfrPeer(true)
	require.Equal(t, dns.RcodeServerFailure, serve(t, c.p.xfr, ix, xfrQuery(zone, 42)))
	require.Empty(t, ix.records())
	c.p.fold(ev)
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	require.Equal(t, uint32(42), c.p.state.serials[zone].Serial, "an unchanged confirmation inside the replay window")
}

// in stamps an event with the stream session the source minted it in.
func in(session uint64, ev *iutil.Event) *iutil.Event { return ev.WithSession(session) }

// TestSecondSpillFIFO: one producer, an inbox of one, and a consumer taking
// the inbox and the spill as Run does. Every event is folded in arrival order.
func TestSecondSpillFIFO(t *testing.T) {
	c := newColdServer(t, InboxSize(1))
	const total = 150000
	events := make([]*iutil.Event, total)
	for i := range events {
		events[i] = iutil.NewEvent(time.Unix(int64(i+1), 0), "stored", "p", "web", "").WithRestored()
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for i, ev := range events {
			c.p.Handle(ev)
			if i%4 == 3 {
				runtime.Gosched()
			}
		}
	}()
	var seen int
	var firstReordered string
	c.p.next = func(ev *iutil.Event) {
		if seen < total && ev != events[seen] && firstReordered == "" {
			firstReordered = fmt.Sprintf("arrival %d folded in position %d", ev.At().Unix(), seen+1)
		}
		seen++
	}
	for seen < total {
		select {
		case ev := <-c.p.inbox:
			c.p.fold(ev)
		case <-c.p.spilled:
			c.p.foldSpilled()
		}
	}
	<-finished
	require.Empty(t, firstReordered)
}

// TestSecondLostConnectionControls: the controls a full inbox drops go on
// past the fold, ahead of what is queued. The events carry the session the
// source minted them in, as it always stamps them, and that is what tells the
// stream is back - not which control arrived last.
//
// Adapted from the review: its events carried no session, which no event the
// source mints lacks; and "first listener never opened" is the source's to
// refuse (TestSecondFirstListenerNeverOpened), since a round with no stream
// open never reaches the chain.
func TestSecondLostConnectionControls(t *testing.T) {
	t.Run("connected dropped by DNS", func(t *testing.T) {
		c := newColdServer(t, InboxSize(1))
		status := readyAfter(t, c.p)
		c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
		c.p.fold(in(1, event(iutil.ActionDisconnected, "", "")))
		c.p.Handle(in(1, event("occupied", "", "")))
		c.p.Handle(in(2, event(iutil.ActionConnected, "", "")))
		c.p.fold(<-c.p.inbox)
		for range 3 {
			c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
		}
		require.Equal(t, 200, status())
		require.True(t, c.p.view.Ready(), "three successful rounds after reconnect")
	})
	t.Run("older disconnect delivered last to HTTP", func(t *testing.T) {
		c := newColdServer(t, InboxSize(1))
		status := readyAfter(t, c.p)
		c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
		c.p.fold(in(1, warm(event(iutil.ActionSweepEnd, "", ""))))
		require.Equal(t, 200, status())
		c.p.Handle(in(1, event(iutil.ActionDisconnected, "", "")))
		c.p.Handle(in(2, event(iutil.ActionConnected, "", "")))
		require.Equal(t, 503, status(), "the reconnection's round has not ended")
		c.p.fold(<-c.p.inbox)
		for range 3 {
			c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
		}
		require.Equal(t, 200, status())
		require.True(t, c.p.view.Ready())
	})
	t.Run("a round of the old session after the reconnection", func(t *testing.T) {
		c := newColdServer(t)
		status := readyAfter(t, c.p)
		c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
		c.p.fold(in(1, event(iutil.ActionDisconnected, "", "")))
		c.p.fold(in(2, event(iutil.ActionConnected, "", "")))
		c.p.fold(in(1, warm(event("instance-updated", "shop", "web"))))
		require.Equal(t, 503, status(), "a warm leftover of the session that went")
		require.False(t, c.p.view.Ready())
	})
}
