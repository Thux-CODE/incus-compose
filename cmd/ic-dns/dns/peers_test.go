package dns

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/cmd/ic-dns/dns/ecs_view"
	"github.com/lxc/incus-compose/ievent/iutil"
)

// The hub topology this file is about, the one a router network and one OVN
// network per project make:
//
//	default/incus-ovn  10.146.11.0/24  mgmt .6, router .40; peered with both apps
//	pvlab1/app         10.147.250.0/24 web .2 (service "django"); peered with the hub
//	pvlab2/app         10.147.251.0/24 db .2 (service "django"); peered with the hub
//
// The two apps are not peered with each other, which is what non-transitivity is
// about: two spokes of one hub must not see each other through it.
const (
	hubNet = "default/incus-ovn"
	app1   = "pvlab1/app"
	app2   = "pvlab2/app"
)

// sitting is one instance on one network its project owns, as the enricher hands
// it over, under the zone suffix ic-dns serves by default.
func sitting(project, name, owner, network, subnet, addr string, meta map[string]string) *instance {
	config := map[string]string{}
	for key, value := range meta {
		config[userLabel(labelPrefix+key)] = value
	}

	ev := event(incusapi.EventLifecycleInstanceStarted, project, name).WithInstance(
		read(project, true, config, map[string]onNet{
			network: {project: owner, subnet: subnet, addrs: []string{addr}},
		}), true)

	return patchInstance(ev, "incus.")
}

// hubFleet is the topology above, by held key.
func hubFleet() map[string]*instance {
	django := map[string]string{metaService: "django"}

	return map[string]*instance{
		"default/mgmt":    sitting("default", "mgmt", "default", "incus-ovn", "10.146.11.0/24", "10.146.11.6", nil),
		"pvlab1/django-1": sitting("pvlab1", "django-1", "pvlab1", "app", "10.147.250.0/24", "10.147.250.2", django),
		"pvlab2/django-1": sitting("pvlab2", "django-1", "pvlab2", "app", "10.147.251.0/24", "10.147.251.2", django),
	}
}

// hubPeers is both spokes peered with the hub, both ways round.
func hubPeers() map[string][]string {
	return map[string][]string{
		hubNet: {app1, app2},
		app1:   {hubNet},
		app2:   {hubNet},
	}
}

// onlyApp1 is the hub peered with the first spoke alone.
func onlyApp1() map[string][]string {
	return map[string][]string{
		hubNet: {app1},
		app1:   {hubNet},
	}
}

// peeredState applies a fleet in one order under a peering.
func peeredState(held map[string]*instance, order []string, peers map[string][]string) *state {
	s := newState(nil)
	s.setPeers(peers, 5)

	for _, key := range order {
		s.apply(key, held[key], 5)
	}

	// Service names are written from every replica at once, as write does.
	s.refresh(5)

	return s
}

// engineOn is an engine serving one snapshot, peer views on or off, behind the
// adapter every query really arrives through.
func engineOn(snap *ecs_view.Snapshot, peerViews bool) *adapter {
	view := ecs_view.New(slog.Default())
	view.PeerViews = peerViews

	wire(newXFR(slog.Default(), nil), view, nil)

	view.Replace(snap)
	view.SetHealthy(true)

	return &adapter{chain: view}
}

// resolve is what a querier at from is answered for one question: the rcode,
// and the answer section as sorted text values.
func resolve(a *adapter, qname string, qtype uint16, from string) (int, []string) {
	m := subnetQuery(qname, from)
	m.Question[0].Qtype = qtype

	w := &recorder{}
	a.ServeDNS(w, m)

	var out []string

	for _, rr := range w.msg.Answer {
		switch rr := rr.(type) {
		case *dns.A:
			out = append(out, rr.A.String())
		case *dns.PTR:
			out = append(out, rr.Ptr)
		default:
			out = append(out, rr.String())
		}
	}

	sort.Strings(out)

	return w.msg.Rcode, out
}

// TestPeerViewsHubMatrix is the lab matrix in miniature: the hub resolves both
// spokes, names and PTRs and service names alike; a spoke resolves itself and
// the hub, and never the other spoke.
func TestPeerViewsHubMatrix(t *testing.T) {
	held := hubFleet()

	s := peeredState(held, slices.Sorted(maps.Keys(held)), hubPeers())
	a := engineOn(s.snapshot(), true)

	const (
		mgmt   = "10.146.11.6"
		router = "10.146.11.40" // on the hub, claimed by nothing: placed by subnet
		web    = "10.147.250.2"
		db     = "10.147.251.2"
		spoke1 = "10.147.250.99"
	)

	cases := []struct {
		from  string
		qname string
		qtype uint16
		want  []string
	}{
		{mgmt, "django-1.pvlab1.incus.", dns.TypeA, []string{web}},
		{mgmt, "django.pvlab1.incus.", dns.TypeA, []string{web}},
		{mgmt, "django-1.pvlab2.incus.", dns.TypeA, []string{db}},
		{mgmt, "django.pvlab2.incus.", dns.TypeA, []string{db}},
		{mgmt, "2.250.147.10.in-addr.arpa.", dns.TypePTR, []string{"django-1.pvlab1.incus."}},
		{mgmt, "2.251.147.10.in-addr.arpa.", dns.TypePTR, []string{"django-1.pvlab2.incus."}},
		{router, "django.pvlab2.incus.", dns.TypeA, []string{db}},
		{web, "django.pvlab1.incus.", dns.TypeA, []string{web}},
		{web, "mgmt.default.incus.", dns.TypeA, []string{mgmt}},
		{web, "6.11.146.10.in-addr.arpa.", dns.TypePTR, []string{"mgmt.default.incus."}},
		{spoke1, "mgmt.default.incus.", dns.TypeA, []string{mgmt}},
		{db, "mgmt.default.incus.", dns.TypeA, []string{mgmt}},
		// Not transitive: both spokes are peered with the hub, not with each other.
		{web, "django-1.pvlab2.incus.", dns.TypeA, nil},
		{web, "django.pvlab2.incus.", dns.TypeA, nil},
		{web, "2.251.147.10.in-addr.arpa.", dns.TypePTR, nil},
		{spoke1, "django.pvlab2.incus.", dns.TypeA, nil},
		{db, "django-1.pvlab1.incus.", dns.TypeA, nil},
	}

	for _, c := range cases {
		rcode, got := resolve(a, c.qname, c.qtype, c.from)

		if c.want == nil {
			assert.Equal(t, dns.RcodeNameError, rcode, "%s asked from %s must not resolve", c.qname, c.from)

			continue
		}

		assert.Equal(t, dns.RcodeSuccess, rcode, "%s asked from %s", c.qname, c.from)
		assert.Equal(t, c.want, got, "%s asked from %s", c.qname, c.from)
	}
}

// TestPeerViewsOffIsTheFilterAsItStands pins the default twice over: an engine
// without the flag answers a peered fold exactly like an unpeered one, and an
// unpeered fold is the tree a fold before this change built.
func TestPeerViewsOffIsTheFilterAsItStands(t *testing.T) {
	held := hubFleet()
	order := slices.Sorted(maps.Keys(held))

	plain := patched(held, 5).snapshot()
	peered := peeredState(held, order, hubPeers()).snapshot()

	off := engineOn(peered, false)
	before := engineOn(plain, false)

	froms := []string{"10.146.11.6", "10.146.11.40", "10.147.250.2", "10.147.251.2", "127.0.0.1"}
	qnames := []string{
		"mgmt.default.incus.", "django-1.pvlab1.incus.", "django.pvlab1.incus.",
		"django-1.pvlab2.incus.", "django.pvlab2.incus.",
		"6.11.146.10.in-addr.arpa.", "2.250.147.10.in-addr.arpa.", "2.251.147.10.in-addr.arpa.",
		"nobody.pvlab1.incus.", "pvlab1.incus.",
	}

	for _, from := range froms {
		for _, qname := range qnames {
			for _, qtype := range []uint16{dns.TypeA, dns.TypeAAAA, dns.TypePTR} {
				wantCode, want := resolve(before, qname, qtype, from)
				gotCode, got := resolve(off, qname, qtype, from)

				assert.Equal(t, wantCode, gotCode, "%s %d from %s", qname, qtype, from)
				assert.Equal(t, want, got, "%s %d from %s", qname, qtype, from)
			}
		}
	}

	// And with no peering at all, the fold writes no peer half anywhere.
	for key, answer := range everyName(plain.Tree) {
		for id := range answer {
			assert.NotContains(t, string(id), "peer", "%s holds a peer half with no peering", key)
		}
	}
}

// peeredFleets are the shapes a peering change has to agree with a build about.
func peeredFleets() map[string]map[string]*instance {
	django := map[string]string{metaService: "django"}

	out := map[string]map[string]*instance{
		"hub and two spokes": hubFleet(),
	}

	// One service with a replica on the hub and one behind a peer, plus a
	// router on both: own and peer halves on one name, and a view spanning a
	// peering.
	mixed := hubFleet()
	mixed["default/django-9"] = sitting("default", "django-9", "default", "incus-ovn", "10.146.11.0/24", "10.146.11.9", django)

	gate := event(incusapi.EventLifecycleInstanceStarted, "pvlab1", "gate").WithInstance(
		read("pvlab1", true, map[string]string{}, map[string]onNet{
			"app":       {project: "pvlab1", subnet: "10.147.250.0/24", addrs: []string{"10.147.250.7"}},
			"incus-ovn": {project: "default", subnet: "10.146.11.0/24", addrs: []string{"10.146.11.7"}},
		}), true)
	mixed["pvlab1/gate"] = patchInstance(gate, "incus.")

	out["a service across a peering and a gate on both"] = mixed

	return out
}

// TestPeeringIsOneTreeHoweverItArrived holds the fan-out to the build: the tree a
// peering change leaves behind is the tree folding the fleet under that peering
// from the start would have, whatever order the fleet came in and whatever
// peering it was folded under first.
func TestPeeringIsOneTreeHoweverItArrived(t *testing.T) {
	for name, held := range peeredFleets() {
		t.Run(name, func(t *testing.T) {
			ref := slices.Sorted(maps.Keys(held))

			want := everyName(peeredState(held, ref, hubPeers()).snapshot().Tree)
			unpeered := everyName(patched(held, 5).snapshot().Tree)

			same := func(got map[string]ecs_view.ViewAnswer, how string) {
				t.Helper()

				require.Len(t, got, len(want), "%s holds a different set of names", how)

				for key, answer := range want {
					mine, ok := got[key]
					require.True(t, ok, "%q is missing after %s", key, how)

					sameAnswers(t, answer, mine, key+" after "+how)
				}
			}

			for _, order := range orders(held) {
				// Peered from the start, in this order.
				same(everyName(peeredState(held, order, hubPeers()).snapshot().Tree),
					fmt.Sprintf("peering first, then %v", order))

				// The fleet first, the peering after.
				s := peeredState(held, order, nil)
				require.True(t, moves(s, hubPeers()))
				same(everyName(s.snapshot().Tree), fmt.Sprintf("%v, then the peering", order))

				// A different peering first, moved to this one: the prefixes the
				// first wrote have to go.
				s = peeredState(held, order, onlyApp1())
				require.True(t, moves(s, hubPeers()))
				same(everyName(s.snapshot().Tree), fmt.Sprintf("%v under one spoke, then both", order))

				// Peered half-way through the arrivals.
				s = newState(nil)
				for i, key := range order {
					if i == len(order)/2 {
						s.setPeers(hubPeers(), 5)
					}

					s.apply(key, held[key], 5)
				}

				s.refresh(5)

				same(everyName(s.snapshot().Tree), fmt.Sprintf("%v, peered half-way", order))

				// And unpeering takes every peer half back out.
				require.True(t, moves(s, nil))

				got := everyName(s.snapshot().Tree)
				require.Len(t, got, len(unpeered), "%v unpeered holds a different set of names", order)

				for key, answer := range unpeered {
					sameAnswers(t, answer, got[key], key+" unpeered")
				}
			}
		})
	}
}

// moves applies a peering and reports whether the table moved.
func moves(s *state, peers map[string][]string) bool {
	changed, _ := s.setPeers(peers, 5)

	return changed
}

// transferred is a zone as a transfer carries it, as text.
func transferred(t *testing.T, s *state, zone string) []string {
	t.Helper()

	snap := s.snapshot()

	z := snap.ZoneOf(zone)
	require.NotNil(t, z, "%s is not served", zone)

	var out []string
	for _, rr := range zoneRecords(snap, z, z.SOA) {
		out = append(out, rr.String())
	}

	return out
}

// TestPeeringChangeNeedsNoRestart pins the lab's step (c) on the fold: one spoke
// unpeered disappears from the hub, the other stays, and peering it again brings
// it back. The zone a secondary transfers does not change - every view folded
// together is the same records under any peering - so its serial does not step.
func TestPeeringChangeNeedsNoRestart(t *testing.T) {
	held := hubFleet()

	s := peeredState(held, slices.Sorted(maps.Keys(held)), hubPeers())
	s.step(true, 5)

	serial := s.serials["pvlab2.incus."].Serial

	ask := func(qname string) int {
		rcode, _ := resolve(engineOn(s.snapshot(), true), qname, dns.TypeA, "10.146.11.6")

		return rcode
	}

	require.Equal(t, dns.RcodeSuccess, ask("django.pvlab2.incus."))

	before := transferred(t, s, "pvlab2.incus.")

	assert.False(t, moves(s, hubPeers()), "the same peering again moves nothing")
	assert.True(t, moves(s, onlyApp1()))
	s.step(true, 5)

	assert.Equal(t, dns.RcodeNameError, ask("django.pvlab2.incus."))
	assert.Equal(t, dns.RcodeNameError, ask("django-1.pvlab2.incus."))
	assert.Equal(t, dns.RcodeSuccess, ask("django.pvlab1.incus."), "the other spoke is untouched")
	assert.Equal(t, serial, s.serials["pvlab2.incus."].Serial, "no record of the zone moved, so no serial step")
	assert.Equal(t, before, transferred(t, s, "pvlab2.incus."), "a transfer reads the same zone")

	assert.True(t, moves(s, hubPeers()))
	assert.Equal(t, dns.RcodeSuccess, ask("django.pvlab2.incus."))
}

// TestEverythingLeavingEmptiesThePeeredTrees is the subtracting patcher's
// invariant with peer halves in it: nothing outlives the instances that wrote it.
func TestEverythingLeavingEmptiesThePeeredTrees(t *testing.T) {
	for name, held := range peeredFleets() {
		t.Run(name, func(t *testing.T) {
			for _, order := range orders(held) {
				s := peeredState(held, order, hubPeers())

				for _, key := range order {
					s.apply(key, nil, 5)
				}

				s.refresh(5)
				s.step(true, 5)

				assert.Empty(t, everyName(s.snapshot().Tree), "a name outlived every instance, applied as %v", order)
				assert.Empty(t, s.nets, "applied as %v", order)
				assert.Empty(t, s.serials, "applied as %v", order)
			}
		})
	}
}

// fakePeers is an Incus that answers the three reads the peering takes, and can
// be changed under a running plugin.
type fakePeers struct {
	mu sync.Mutex

	projects []incusapi.Project
	networks map[string][]incusapi.Network
	peers    map[string][]incusapi.NetworkPeer

	// fail is what a read of one project's networks, or one network's peers,
	// answers instead.
	fail map[string]error

	// down is what every read answers instead, a daemon that stopped answering.
	down error

	reads int
}

func (f *fakePeers) GetProjects(_ context.Context) ([]incusapi.Project, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.reads++

	if f.down != nil {
		return nil, f.down
	}

	return slices.Clone(f.projects), nil
}

func (f *fakePeers) GetNetworks(_ context.Context, project string) ([]incusapi.Network, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	err := f.fail[project]
	if err != nil {
		return nil, err
	}

	return slices.Clone(f.networks[project]), nil
}

func (f *fakePeers) GetNetworkPeers(_ context.Context, project, network string) ([]incusapi.NetworkPeer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := iutil.NetworkKey(project, network)

	err := f.fail[key]
	if err != nil {
		return nil, err
	}

	return slices.Clone(f.peers[key]), nil
}

// peer is one side of a peering as Incus lists it.
func peer(name, project, network, status string) incusapi.NetworkPeer {
	return incusapi.NetworkPeer{
		Name: name, TargetProject: project, TargetNetwork: network, Status: status, Type: "local",
	}
}

// ovn is one managed OVN network.
func ovn(name string) incusapi.Network {
	return incusapi.Network{Name: name, Managed: true, Type: "ovn"}
}

// project is one project, with networks of its own or not.
func project(name string, ownNetworks bool) incusapi.Project {
	p := incusapi.Project{Name: name}
	p.Config = map[string]string{}

	if ownNetworks {
		p.Config[iutil.FeaturesNetworks] = "true"
	}

	return p
}

// hubIncus is the hub topology as Incus would list it, both peerings up.
func hubIncus() *fakePeers {
	return &fakePeers{
		projects: []incusapi.Project{project("default", false), project("pvlab1", true), project("pvlab2", true)},
		networks: map[string][]incusapi.Network{
			"default": {ovn("incus-ovn"), {Name: "incus-uplink", Managed: true, Type: "bridge"}},
			"pvlab1":  {ovn("app")},
			"pvlab2":  {ovn("app")},
		},
		peers: map[string][]incusapi.NetworkPeer{
			hubNet: {
				peer("to-pvlab1", "pvlab1", "app", incusapi.NetworkStatusCreated),
				peer("to-pvlab2", "pvlab2", "app", incusapi.NetworkStatusCreated),
			},
			app1: {peer("to-ovn", "default", "incus-ovn", incusapi.NetworkStatusCreated)},
			app2: {peer("to-ovn", "default", "incus-ovn", incusapi.NetworkStatusCreated)},
		},
		fail: map[string]error{},
	}
}

func TestReadPeers(t *testing.T) {
	t.Parallel()

	refusedErr := incusapi.StatusErrorf(http.StatusForbidden, "not authorized")

	cases := []struct {
		name   string
		change func(f *fakePeers)
		serves func(*incusapi.Project) bool
		want   map[string][]string
		err    bool
	}{
		{
			name: "both sides created is a pair",
			want: hubPeers(),
		},
		{
			name: "one side pending is no pair",
			change: func(f *fakePeers) {
				f.peers[app2] = []incusapi.NetworkPeer{peer("to-ovn", "default", "incus-ovn", "Pending")}
			},
			want: onlyApp1(),
		},
		{
			name:   "one side missing is no pair",
			change: func(f *fakePeers) { delete(f.peers, app2) },
			want:   onlyApp1(),
		},
		{
			name: "a project not served is never read",
			serves: func(p *incusapi.Project) bool {
				return p.Name != "pvlab2"
			},
			want: onlyApp1(),
		},
		{
			name:   "a side the certificate may not read is dropped, not fatal",
			change: func(f *fakePeers) { f.fail["pvlab2"] = refusedErr },
			want:   onlyApp1(),
		},
		{
			name:   "a network gone between list and read is dropped, not fatal",
			change: func(f *fakePeers) { f.fail[app2] = incusapi.StatusErrorf(http.StatusNotFound, "not found") },
			want:   onlyApp1(),
		},
		{
			name:   "anything else fails the read, so what is held stays",
			change: func(f *fakePeers) { f.fail[app2] = errors.New("connection reset") },
			err:    true,
		},
		{
			name: "a project sharing the default networks is not read under its own name",
			change: func(f *fakePeers) {
				f.projects = append(f.projects, project("shared", false))
				f.networks["shared"] = []incusapi.Network{ovn("incus-ovn")}
				f.peers["shared/incus-ovn"] = []incusapi.NetworkPeer{peer("x", "pvlab1", "app", incusapi.NetworkStatusCreated)}
				f.peers[app1] = append(f.peers[app1], peer("y", "shared", "incus-ovn", incusapi.NetworkStatusCreated))
			},
			want: hubPeers(),
		},
		{
			name: "a peer towards an integration points at no network",
			change: func(f *fakePeers) {
				remote := peer("ic", "pvlab2", "app", incusapi.NetworkStatusCreated)
				remote.Type = "remote"
				f.peers[app1] = append(f.peers[app1], remote)
				f.peers[app2] = append(f.peers[app2], peer("back", "pvlab1", "app", incusapi.NetworkStatusCreated))
			},
			want: hubPeers(),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			f := hubIncus()
			if c.change != nil {
				c.change(f)
			}

			got, err := readPeers(t.Context(), f, c.serves)
			if c.err {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}

// TestPeerViewsOffWantsNothingNew pins that the flag off reads nothing and
// subscribes to nothing new: the chain sees exactly the actions it saw before.
func TestPeerViewsOffWantsNothingNew(t *testing.T) {
	t.Parallel()

	off := New(slog.Default())
	on := New(slog.Default(), PeerViews(true))

	for _, w := range off.Wants() {
		assert.NotContains(t, peerActions, w.Action)
	}

	assert.Len(t, on.Wants(), len(off.Wants())+len(peerActions))

	for _, w := range on.Wants() {
		if slices.Contains(peerActions, w.Action) {
			assert.False(t, w.Debounce, "%s keys like an instance in debounce, so it may not collapse", w.Action)
			assert.Zero(t, w.Enrich, "%s needs no read", w.Action)
		}
	}

	// Asked, it does not ask: there is nobody to read.
	off.askPeers()
	assert.Empty(t, off.peerAsk)
}

// TestPluginFollowsThePeering runs the plugin the way main does, with Incus
// faked: the first read peers both spokes, a peer event re-reads, and the hub
// stops resolving the unpeered spoke within the read - no restart in between.
func TestPluginFollowsThePeering(t *testing.T) {
	f := hubIncus()

	r := &running{
		p:      New(slog.Default(), PeerViews(true), TTL(30), Suffix("incus")),
		seen:   make(chan *iutil.Event, 64),
		in:     make(chan iutil.Command),
		raised: make(chan iutil.Command, 16),
		done:   make(chan error, 1),
	}

	r.p.peerReader = f

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	require.NoError(t, r.p.Setup(iutil.SetupArgs{
		Next:       func(ev *iutil.Event) { r.seen <- ev },
		CommandIn:  r.in,
		CommandOut: r.raised,
	}))

	go func() { r.done <- r.p.Run(ctx) }()

	var feed []*iutil.Event
	for _, inst := range hubFleet() {
		feed = append(feed, inst.Event)
	}

	feed = append(feed, event(iutil.ActionSweepEnd, "", ""))
	r.feed(t, feed...)

	a := &adapter{chain: r.p.view}

	eventually := func(qname string, want int, why string) {
		t.Helper()

		deadline := time.Now().Add(3 * time.Second)

		for {
			rcode, _ := resolve(a, qname, dns.TypeA, "10.146.11.6")
			if rcode == want {
				return
			}

			if time.Now().After(deadline) {
				t.Fatalf("%s: %s answered rcode %d, want %d", why, qname, rcode, want)
			}

			time.Sleep(20 * time.Millisecond)
		}
	}

	eventually("django.pvlab2.incus.", dns.RcodeSuccess, "peered at start")
	eventually("django.pvlab1.incus.", dns.RcodeSuccess, "peered at start")

	// The peering goes in Incus, and its event arrives.
	f.mu.Lock()
	f.peers[hubNet] = f.peers[hubNet][:1]
	delete(f.peers, app2)
	f.mu.Unlock()

	r.feed(t, warm(event(incusapi.EventLifecycleNetworkPeerDeleted, "default", "to-pvlab2")))

	eventually("django.pvlab2.incus.", dns.RcodeNameError, "unpeered")
	eventually("django.pvlab1.incus.", dns.RcodeSuccess, "the other spoke")

	// And back.
	f.mu.Lock()
	f.peers = hubIncus().peers
	f.mu.Unlock()

	r.feed(t, warm(event(incusapi.EventLifecycleNetworkPeerCreated, "pvlab2", "to-ovn")))

	eventually("django.pvlab2.incus.", dns.RcodeSuccess, "peered again")

	r.stop(t)

	f.mu.Lock()
	defer f.mu.Unlock()

	assert.GreaterOrEqual(t, f.reads, 3, "read at start and once per peer event")
}

// runPeered starts the plugin the way main does, peer views on, with Incus
// faked by f and a short retry so a test does not wait out the real one.
func runPeered(t *testing.T, f *fakePeers, opts ...Option) *running {
	t.Helper()

	r := &running{
		p:      New(slog.Default(), append([]Option{PeerViews(true)}, opts...)...),
		seen:   make(chan *iutil.Event, 64),
		in:     make(chan iutil.Command),
		raised: make(chan iutil.Command, 16),
		done:   make(chan error, 1),
	}

	r.p.peerReader = f
	r.p.peerRetry = [2]time.Duration{20 * time.Millisecond, 80 * time.Millisecond}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	require.NoError(t, r.p.Setup(iutil.SetupArgs{
		Next:       func(ev *iutil.Event) { r.seen <- ev },
		CommandIn:  r.in,
		CommandOut: r.raised,
	}))

	go func() { r.done <- r.p.Run(ctx) }()

	return r
}

// readsOf is how many reads of the peering f has answered.
func readsOf(f *fakePeers) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.reads
}

// TestColdPeeringKeepsTheSerials is TestColdBecomesWarm with the peering read
// landing before the replay: a peering with nothing on it rewrites no name,
// so it publishes nothing, and the serials a previous run stored survive.
func TestColdPeeringKeepsTheSerials(t *testing.T) {
	const zone = "shop.example."

	seed, err := encodeCold(map[string]zoneSerial{zone: {Serial: 7}}, nil)
	require.NoError(t, err)

	f := hubIncus()
	r := runPeered(t, f, Suffix("example"), ColdMemory(seed))

	// The first read lands on an empty fleet, ahead of everything.
	require.Eventually(t, func() bool { return readsOf(f) >= 1 }, 2*time.Second, 5*time.Millisecond)

	time.Sleep(50 * time.Millisecond)

	r.feed(t,
		eventOn(incusapi.EventLifecycleInstanceStarted, "shop", "web", nil,
			map[string]string{"net-a": "10.0.0.5"}),
		event(iutil.ActionSweepEnd, "", ""))

	r.feed(t,
		warm(eventOn(incusapi.EventLifecycleInstanceUpdated, "shop", "web", nil,
			map[string]string{"net-a": "10.0.0.9"})),
		warm(event(iutil.ActionSweepEnd, "", "")))

	r.stop(t)

	assert.Equal(t, uint32(8), r.p.state.serials[zone].Serial,
		"the replay claimed nothing and the warm change claimed once")

	back, _, err := decodeCold(r.p.cold.held)
	require.NoError(t, err)
	assert.Equal(t, uint32(8), back[zone].Serial)
}

// TestAFailedPeerReadIsRetried pins what a hiccup costs: the peering held is
// served while incusd does not answer, and the change that arrived meanwhile
// lands once it answers again - with no further event to ask for it.
func TestAFailedPeerReadIsRetried(t *testing.T) {
	f := hubIncus()
	r := runPeered(t, f, TTL(30), Suffix("incus"))

	var feed []*iutil.Event
	for _, inst := range hubFleet() {
		feed = append(feed, inst.Event)
	}

	r.feed(t, append(feed, event(iutil.ActionSweepEnd, "", ""))...)

	a := &adapter{chain: r.p.view}

	answers := func() int {
		rcode, _ := resolve(a, "django.pvlab2.incus.", dns.TypeA, "10.146.11.6")

		return rcode
	}

	require.Eventually(t, func() bool { return answers() == dns.RcodeSuccess },
		2*time.Second, 10*time.Millisecond, "peered at start")

	// incusd stops answering, and the peering goes while it does.
	f.mu.Lock()
	f.down = errors.New("connection refused")
	f.peers[hubNet] = f.peers[hubNet][:1]
	delete(f.peers, app2)
	f.mu.Unlock()

	before := readsOf(f)

	r.feed(t, warm(event(incusapi.EventLifecycleNetworkPeerDeleted, "default", "to-pvlab2")))

	require.Eventually(t, func() bool { return readsOf(f) >= before+3 },
		2*time.Second, 5*time.Millisecond, "a failed read is tried again by itself")
	assert.Equal(t, dns.RcodeSuccess, answers(), "what is held is served while nothing can be read")

	// It answers again: no event, the retry alone brings the change in.
	f.mu.Lock()
	f.down = nil
	f.mu.Unlock()

	require.Eventually(t, func() bool { return answers() == dns.RcodeNameError },
		2*time.Second, 10*time.Millisecond, "the retry read the peering that went")

	r.stop(t)
}

// alternative is one instance of peeredFleets changed in what decides its
// records: a new address, and for the gate one network fewer.
func alternative(key string) *instance {
	django := map[string]string{metaService: "django"}

	switch key {
	case "default/mgmt":
		return sitting("default", "mgmt", "default", "incus-ovn", "10.146.11.0/24", "10.146.11.16", nil)
	case "default/django-9":
		return sitting("default", "django-9", "default", "incus-ovn", "10.146.11.0/24", "10.146.11.19", django)
	case "pvlab1/django-1":
		return sitting("pvlab1", "django-1", "pvlab1", "app", "10.147.250.0/24", "10.147.250.12", django)
	case "pvlab2/django-1":
		return sitting("pvlab2", "django-1", "pvlab2", "app", "10.147.251.0/24", "10.147.251.12", django)
	case "pvlab1/gate":
		return sitting("pvlab1", "gate", "pvlab1", "app", "10.147.250.0/24", "10.147.250.7", nil)
	}

	panic("no alternative for " + key)
}

// sameTree requires two folds to answer every name the same way.
func sameTree(t *testing.T, want, got *state, how string) {
	t.Helper()

	w := everyName(want.snapshot().Tree)
	g := everyName(got.snapshot().Tree)

	require.Len(t, g, len(w), "%s holds a different set of names", how)

	for key, answer := range w {
		mine, ok := g[key]
		require.True(t, ok, "%q is missing after %s", key, how)

		sameAnswers(t, answer, mine, key+" after "+how)
	}
}

// TestOneInstanceMovesUnderPeering holds a single patch under a peering to a
// fold from scratch: one instance changing, or leaving, while the rest stay.
func TestOneInstanceMovesUnderPeering(t *testing.T) {
	for name, held := range peeredFleets() {
		t.Run(name, func(t *testing.T) {
			order := slices.Sorted(maps.Keys(held))

			for _, key := range order {
				changed := maps.Clone(held)
				changed[key] = alternative(key)

				s := peeredState(held, order, hubPeers())
				s.apply(key, changed[key], 5)
				s.refresh(5)
				sameTree(t, peeredState(changed, order, hubPeers()), s, key+" changed")

				gone := maps.Clone(held)
				delete(gone, key)

				rest := slices.DeleteFunc(slices.Clone(order), func(k string) bool { return k == key })

				s.apply(key, nil, 5)
				s.refresh(5)
				sameTree(t, peeredState(gone, rest, hubPeers()), s, key+" gone")
			}
		})
	}
}

// TestNetworkRenamedOrDeletedUnderPeering is a peered network moving with
// instances still on it. The enricher re-reads the instances and the peering
// is read again, in either order; both end where a fold from scratch does.
func TestNetworkRenamedOrDeletedUnderPeering(t *testing.T) {
	django := map[string]string{metaService: "django"}
	held := peeredFleets()["a service across a peering and a gate on both"]
	order := slices.Sorted(maps.Keys(held))

	gateOn := func(nets map[string]onNet) *instance {
		ev := event(incusapi.EventLifecycleInstanceStarted, "pvlab1", "gate").WithInstance(
			read("pvlab1", true, map[string]string{}, nets), true)

		return patchInstance(ev, "incus.")
	}

	hubSide := onNet{project: "default", subnet: "10.146.11.0/24", addrs: []string{"10.146.11.7"}}

	cases := map[string]struct {
		moved map[string]*instance
		peers map[string][]string
	}{
		"pvlab1/app renamed to web": {
			moved: map[string]*instance{
				"pvlab1/django-1": sitting("pvlab1", "django-1", "pvlab1", "web", "10.147.250.0/24", "10.147.250.2", django),
				"pvlab1/gate": gateOn(map[string]onNet{
					"web":       {project: "pvlab1", subnet: "10.147.250.0/24", addrs: []string{"10.147.250.7"}},
					"incus-ovn": hubSide,
				}),
			},
			peers: map[string][]string{
				hubNet:       {"pvlab1/web", app2},
				"pvlab1/web": {hubNet},
				app2:         {hubNet},
			},
		},
		"pvlab1/app deleted": {
			moved: map[string]*instance{
				"pvlab1/django-1": patchInstance(event(incusapi.EventLifecycleInstanceStarted, "pvlab1", "django-1").
					WithInstance(read("pvlab1", true, map[string]string{
						userLabel(labelPrefix + metaService): "django",
					}, map[string]onNet{}), true), "incus."),
				"pvlab1/gate": gateOn(map[string]onNet{"incus-ovn": hubSide}),
			},
			peers: map[string][]string{hubNet: {app2}, app2: {hubNet}},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			after := maps.Clone(held)
			maps.Copy(after, c.moved)

			want := peeredState(after, order, c.peers)

			// Instances re-read first, the peering after.
			s := peeredState(held, order, hubPeers())
			for key, inst := range c.moved {
				s.apply(key, inst, 5)
			}

			s.refresh(5)
			moves(s, c.peers)
			sameTree(t, want, s, "instances then peering")

			// The peering first.
			s = peeredState(held, order, hubPeers())
			moves(s, c.peers)

			for key, inst := range c.moved {
				s.apply(key, inst, 5)
			}

			s.refresh(5)
			sameTree(t, want, s, "peering then instances")
		})
	}
}

// TestPeeringIsReadAgainOnWhatCanMoveIt pins the triggers: the peer events, a
// network appearing, going or being renamed, the stream coming back, and the
// end of every round. Nothing else asks, and nothing asks with the flag off.
func TestPeeringIsReadAgainOnWhatCanMoveIt(t *testing.T) {
	asks := []*iutil.Event{
		event(iutil.ActionConnected, "", ""),
		event(iutil.ActionSweepEnd, "", ""),
		event(incusapi.EventLifecycleNetworkCreated, "pvlab1", "app"),
		event(incusapi.EventLifecycleNetworkDeleted, "pvlab1", "app"),
		iutil.NewEvent(time.Now(), incusapi.EventLifecycleNetworkRenamed, "pvlab1", "web", "app"),
		event(incusapi.EventLifecycleNetworkPeerCreated, "pvlab1", "to-ovn"),
		event(incusapi.EventLifecycleNetworkPeerUpdated, "pvlab1", "to-ovn"),
		event(incusapi.EventLifecycleNetworkPeerDeleted, "pvlab1", "to-ovn"),
	}

	quiet := []*iutil.Event{
		event(iutil.ActionDisconnected, "", ""),
		event(incusapi.EventLifecycleNetworkUpdated, "pvlab1", "app"),
		enriched(incusapi.EventLifecycleInstanceStarted, "pvlab1", "web", "10.0.0.2"),
	}

	// Folded straight, as answering does: nothing runs, so nothing reads the
	// asks but this test.
	on := New(slog.Default(), PeerViews(true))
	on.next = func(_ *iutil.Event) {}

	off := New(slog.Default())
	off.next = func(_ *iutil.Event) {}

	for _, ev := range asks {
		on.fold(ev)
		assert.Len(t, on.peerAsk, 1, "%s asks for a read", ev.Action())

		<-on.peerAsk

		off.fold(ev)
		assert.Empty(t, off.peerAsk, "%s asks nothing with the flag off", ev.Action())
	}

	for _, ev := range quiet {
		on.fold(ev)
		assert.Empty(t, on.peerAsk, "%s asks for no read", ev.Action())
	}
}

// TestDualHomedIsNotABridge pins non-transitivity where it is easiest to get
// wrong: an instance on both the hub and a spoke does not carry the spoke to
// the other spoke. The other spoke reaches it on the hub, and only there.
func TestDualHomedIsNotABridge(t *testing.T) {
	held := peeredFleets()["a service across a peering and a gate on both"]

	a := engineOn(peeredState(held, slices.Sorted(maps.Keys(held)), hubPeers()).snapshot(), true)

	const (
		db   = "10.147.251.2"
		web  = "10.147.250.2"
		gate = "10.146.11.7"
	)

	cases := []struct {
		from  string
		qname string
		qtype uint16
		want  []string
	}{
		{db, "gate.pvlab1.incus.", dns.TypeA, []string{"10.146.11.7"}},
		{db, "7.11.146.10.in-addr.arpa.", dns.TypePTR, []string{"gate.pvlab1.incus."}},
		{db, "7.250.147.10.in-addr.arpa.", dns.TypePTR, nil},
		{db, "django-1.pvlab1.incus.", dns.TypeA, nil},
		{db, "django.default.incus.", dns.TypeA, []string{"10.146.11.9"}},
		{web, "gate.pvlab1.incus.", dns.TypeA, []string{"10.146.11.7", "10.147.250.7"}},
		{web, "django-1.pvlab2.incus.", dns.TypeA, nil},
		// The gate sits on the hub, so it sees the hub's peers one hop away.
		{gate, "django-1.pvlab2.incus.", dns.TypeA, []string{db}},
		{gate, "django-1.pvlab1.incus.", dns.TypeA, []string{web}},
	}

	for _, c := range cases {
		rcode, got := resolve(a, c.qname, c.qtype, c.from)

		if c.want == nil {
			assert.Equal(t, dns.RcodeNameError, rcode, "%s from %s", c.qname, c.from)

			continue
		}

		assert.Equal(t, dns.RcodeSuccess, rcode, "%s from %s", c.qname, c.from)
		assert.Equal(t, c.want, got, "%s from %s", c.qname, c.from)
	}
}
