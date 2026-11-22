package dns

import (
	"net/netip"
	"testing"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

func TestFifthServiceSelectionAtLegacyCompare(t *testing.T) {
	t.Skip("known issue: a legacy confirmation that changes which replica a service picks does not step the serial (N9)")
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	c.p.state = newState(map[string]zoneSerial{zone: {Serial: 42}})
	instance := func(n, ip string) *iutil.Event {
		return eventOn(api.EventLifecycleInstanceUpdated, "shop", n, map[string]string{metaService: "api"}, map[string]string{"a": ip}).WithProject(iutil.NewProject(optedIn()))
	}
	a := instance("one", "10.0.0.2")
	b := instance("two", "10.0.0.3")
	c.p.fold(a.WithRestored())
	c.p.fold(b.WithRestored())
	c.p.write()
	queryService := func() int {
		w := &recorder{}
		c.a.ServeDNS(w, subnetQuery("api."+zone, "10.0.0.2"))
		require.Equal(t, dns.RcodeSuccess, w.msg.Rcode)
		return len(w.msg.Answer)
	}
	require.Equal(t, 2, queryService())
	c.p.fold(a)
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	require.Equal(t, 1, queryService())
	peer := xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, peer, xfrQuery(zone, 42)))
	require.Greater(t, len(peer.records()), 1, "IXFR from 42: %v", peer.records())
}

func TestFifthAliasChangeLeavesReverseZoneAlone(t *testing.T) {
	t.Skip("known issue: a forward alias change marks every zone of the instance at the legacy compare, the unchanged reverse included (N19)")
	const rev = "0.0.10.in-addr.arpa."
	c := newColdServer(t, Suffix("example"), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	c.p.state = newState(map[string]zoneSerial{rev: {Serial: 42}})
	web := func(alias string) *iutil.Event {
		return eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", map[string]string{metaAliases: alias}, map[string]string{"a": "10.0.0.2"}).WithProject(iutil.NewProject(optedIn()))
	}
	c.p.fold(web("one").WithRestored())
	c.p.write()
	c.p.fold(web("two"))
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	peer := xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, peer, xfrQuery(rev, 42)))
	require.Len(t, peer.records(), 1, "unchanged reverse zone: %v", peer.records())
}

func TestFifthOldDeleteAfterCurrentRound(t *testing.T) {
	c := newColdServer(t, Suffix("example"))
	c.p.fold(in(2, eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": "10.0.0.2"})))
	c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
	ask := func() int {
		w := &recorder{}
		c.a.ServeDNS(w, subnetQuery("web.shop.example.", "10.0.0.2"))
		return w.msg.Rcode
	}
	require.Equal(t, dns.RcodeSuccess, ask())
	c.p.fold(in(1, event(api.EventLifecycleInstanceDeleted, "shop", "web")))
	c.p.write()
	require.Equal(t, dns.RcodeSuccess, ask(), "older listener removed a current record")
}

func TestFifthChildSerialForDottedService(t *testing.T) {
	t.Skip("known issue: a dotted service name moves the serial of the instance's zone, not of the child zone that transfers it (N12)")
	const child = "api.shop.example."
	c := newColdServer(t, Suffix("example"), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	web := func(ip string) *iutil.Event {
		return eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", map[string]string{metaService: "www.api"}, map[string]string{"a": ip})
	}
	c.p.fold(web("10.0.0.2"))
	c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "shop", "worker", map[string]string{metaZone: child}, map[string]string{"a": "10.0.0.3"}).WithProject(iutil.NewProject(optedIn())))
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	old := c.p.state.serials[child].Serial
	c.p.fold(web("10.0.0.9"))
	c.p.write()
	peer := xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, peer, xfrQuery(child, 0)))
	found := false
	for _, rr := range peer.records() {
		a, ok := rr.(*dns.A)
		if ok && a.Hdr.Name == "www."+child && a.A.String() == "10.0.0.9" {
			found = true
		}
	}
	require.True(t, found)
	peer = xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, peer, xfrQuery(child, old)))
	require.Greater(t, len(peer.records()), 1, "new address visible in child AXFR, IXFR says unchanged")
}

func TestFifthLegacyChildGoneBeforeFirstPublish(t *testing.T) {
	t.Skip("known issue: a legacy child zone gone before the first publish does not step the parent that takes its names (N11)")
	const parent = "shop.example."
	const child = "api." + parent
	c := newColdServer(t, Suffix("example"), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	c.p.state = newState(map[string]zoneSerial{parent: {Serial: 42}, child: {Serial: 71}})
	web := eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", map[string]string{metaAliases: "v1.api"}, map[string]string{"a": "10.0.0.2"}).WithProject(iutil.NewProject(optedIn()))
	worker := eventOn(api.EventLifecycleInstanceUpdated, "other", "worker", map[string]string{metaZone: child}, map[string]string{"a": "10.0.0.3"})
	c.p.fold(web.WithRestored())
	c.p.fold(worker.WithRestored())
	c.p.fold(event(api.EventLifecycleInstanceDeleted, "other", "worker"))
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	peer := xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, peer, xfrQuery(parent, 0)))
	hasAlias := false
	for _, rr := range peer.records() {
		if rr.Header().Name == "v1."+child {
			hasAlias = true
		}
	}
	require.True(t, hasAlias)
	peer = xfrPeer(true)
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, peer, xfrQuery(parent, 42)))
	require.Greater(t, len(peer.records()), 1, "parent gained the alias")
}

func TestFifthPeeringBeforeLegacyReplay(t *testing.T) {
	t.Skip("known issue: a peering read before the legacy replay changes the replicas served without stepping the serial (N10)")
	const zone = "shop.example."
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			c := newColdServer(t, Suffix("example"), PeerViews(true), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
			c.p.state = newState(map[string]zoneSerial{zone: {Serial: 42}})
			ka, kb := onWire("net-a"), onWire("net-b")
			// The stored peering covered both networks via two unrelated wires.
			// Reading only stored targets before any instance replay is possible.
			prev := map[string][]string{ka: {onWire("c")}, onWire("c"): {ka}, kb: {onWire("d")}, onWire("d"): {kb}}
			c.p.state.setPeers(prev, c.p.cfg.TTL)
			c.p.knownPeers = prev
			c.p.peersRestored = true
			pr := peerRead{keys: c.p.state.peerTargets(), peers: map[string][]string{ka: {kb}, kb: {ka}}}
			if phase == "before" {
				c.p.peered(pr)
			}
			active := replica(api.EventLifecycleInstanceUpdated, "one", "10.0.0.2", true, "healthy")
			conf := map[string]string{userLabel(labelServiceCompose): "frontend", "user.healthcheck.enabled": "true", "user.healthcheck.test": healthTest, "user.healthcheck.status": "unhealthy"}
			idle := event(api.EventLifecycleInstanceUpdated, "shop", "two").WithInstance(read("shop", true, conf, netsOf(map[string]string{"net-b": "10.1.0.3"})), true)
			for _, ev := range []*iutil.Event{active, idle} {
				c.p.fold(ev.WithProject(iutil.NewProject(optedIn())).WithRestored())
			}
			c.p.write()
			if phase == "after" {
				c.p.peered(pr)
			}
			c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
			peer := xfrPeer(true)
			require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, peer, xfrQuery(zone, 0)))
			var ips []string
			for _, rr := range peer.records() {
				a, ok := rr.(*dns.A)
				if ok && a.Hdr.Name == "frontend."+zone {
					ips = append(ips, a.A.String())
				}
			}
			require.Equal(t, []string{"10.0.0.2"}, ips)
			peer = xfrPeer(true)
			require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, peer, xfrQuery(zone, 42)))
			require.Greater(t, len(peer.records()), 1, "peering changes the service from two addresses to one")
		})
	}
}

func TestFifthFirstPeerAtFullInbox(t *testing.T) {
	c := newColdServer(t, InboxSize(1), PeerViews(true), Suffix("example"))
	c.p.peersRead.Store(true)
	status := readyAfter(t, c.p)
	c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(1, eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": "10.0.0.2"})))
	c.p.fold(in(1, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.True(t, c.p.view.Ready())
	c.p.Handle(in(1, event("backlog", "", "")))
	c.p.Handle(in(2, event(peerActions[0], "p", "net")))
	c.p.foldSpilled()
	require.Equal(t, 503, status())
	w := &recorder{}
	c.a.ServeDNS(w, subnetQuery("web.shop.example.", "10.0.0.2"))
	require.NotEmpty(t, w.msg.Answer)
	require.LessOrEqual(t, w.msg.Answer[0].Header().Ttl, uint32(5), "unconfirmed record still served with its full TTL")
	require.False(t, c.p.view.Ready(), "HTTP saw session 2 while the view still considers session 1 fresh")
}
