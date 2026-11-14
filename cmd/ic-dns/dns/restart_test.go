package dns

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coredns/coredns/plugin"
	"github.com/gorilla/websocket"
	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/iclient"
	"github.com/lxc/incus-compose/ievent/enricher"
	ihttp "github.com/lxc/incus-compose/ievent/http"
	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/ievent/source"
	"github.com/lxc/incus-compose/internal/testlib"
)

// The addresses the restart below is asked from.
const (
	// shopQuerier sits on shop's network and is no instance: a resolver relaying
	// for something on it, placed by the subnet rather than by a name.
	shopQuerier = "10.0.0.50"

	// edgeRouter is the edge, on none of the fleet's networks and trusted.
	edgeRouter = "10.146.11.40"
)

// gatedIncus is an Incus that holds every request, the event stream included,
// until open is closed - a daemon slower than the whole cold phase - and from
// then on answers the fleet it was given. Over a unix socket, as stalledIncus.
type gatedIncus struct {
	open    chan struct{}
	release func()

	fleet map[string]*testlib.Project
}

func newGatedIncus(t *testing.T, fleet ...*testlib.Project) (*gatedIncus, *iclient.Connection) {
	t.Helper()

	g := &gatedIncus{open: make(chan struct{}), fleet: map[string]*testlib.Project{}}

	var once sync.Once
	g.release = func() { once.Do(func() { close(g.open) }) }

	for _, p := range fleet {
		g.fleet[p.Project.Name] = p
	}

	dir, err := os.MkdirTemp("", "ic-dns-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	socket := filepath.Join(dir, "incus.sock")

	listener, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	require.NoError(t, err)

	srv := &http.Server{ReadHeaderTimeout: time.Second, Handler: g}

	go func() { _ = srv.Serve(listener) }()

	t.Cleanup(func() {
		g.release()
		_ = srv.Close()
	})

	conn, err := iclient.NewConnection(&iclient.ConfigRemoteInfo{Name: "gated", Addrs: []string{"unix:" + socket}})
	require.NoError(t, err)

	return g, conn
}

// ServeHTTP answers the handful of reads the source and the enricher make.
func (g *gatedIncus) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	select {
	case <-g.open:
	case <-r.Context().Done():
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/1.0")
	project := g.fleet[r.URL.Query().Get("project")]

	if path == "/events" {
		g.events(w, r)

		return
	}

	if path == "/projects" {
		names := []string{}
		for name := range g.fleet {
			names = append(names, "/1.0/projects/"+name)
		}

		incusReply(w, names)

		return
	}

	if name, ok := strings.CutPrefix(path, "/projects/"); ok {
		p, known := g.fleet[name]
		if !known {
			incusMissing(w)

			return
		}

		incusReply(w, p.Project)

		return
	}

	// Anything else is scoped by a project, and a project not in the fleet -
	// default, most of all - holds nothing.
	if project == nil {
		project = &testlib.Project{}
	}

	if path == "/networks" {
		names := []string{}
		for _, n := range project.Networks {
			names = append(names, "/1.0/networks/"+n.Name)
		}

		incusReply(w, names)

		return
	}

	if name, ok := strings.CutPrefix(path, "/networks/"); ok {
		for _, n := range project.Networks {
			if n.Name == name {
				incusReply(w, n)

				return
			}
		}

		incusMissing(w)

		return
	}

	if path == "/instances" {
		names := []string{}
		for _, i := range project.Instances {
			names = append(names, "/1.0/instances/"+i.Name)
		}

		incusReply(w, names)

		return
	}

	if rest, ok := strings.CutPrefix(path, "/instances/"); ok {
		name, state := strings.CutSuffix(rest, "/state")

		for _, i := range project.Instances {
			if i.Name != name {
				continue
			}

			if state {
				incusReply(w, project.States[name])
			} else {
				incusReply(w, incusapi.InstanceFull{Instance: i})
			}

			return
		}
	}

	incusMissing(w)
}

// events holds the stream open and says nothing on it: the sweep is what reads
// the fleet, and a quiet stream is all a connected one needs to be.
func (g *gatedIncus) events(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}

	socket, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	defer func() { _ = socket.Close() }()

	// Until the listener closes its end.
	for {
		_, _, err := socket.ReadMessage()
		if err != nil {
			return
		}
	}
}

// incusReply is a sync response, the envelope iclient decodes.
func incusReply(w http.ResponseWriter, metadata any) {
	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":        "sync",
		"status":      "Success",
		"status_code": http.StatusOK,
		"metadata":    metadata,
	})
}

// incusMissing is the error a read of something Incus does not hold answers with.
func incusMissing(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)

	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":       "error",
		"error":      "not found",
		"error_code": http.StatusNotFound,
	})
}

// liveProject is one project as Incus answers it: networks of its own, and
// each instance on the project's only network holding the address given.
func liveProject(name string, network int, instances map[string]string, config map[string]map[string]string) *testlib.Project {
	p := testlib.NewProject(name, 0, 0)
	p.Project.Config[iutil.FeaturesNetworks] = "true"
	p.Networks = []incusapi.Network{testlib.NewNetwork(name, network)}

	for instance, addr := range instances {
		inst := testlib.NewInstance(name, 0, network)
		inst.Name = instance

		for k, v := range config[instance] {
			inst.Config[k] = v
			inst.ExpandedConfig[k] = v
		}

		state := testlib.NewInstanceState(0, network)
		eth0 := state.Network["eth0"]
		eth0.Addresses[0].Address = addr
		state.Network["eth0"] = eth0

		p.Instances = append(p.Instances, inst)
		p.States[instance] = state
	}

	return p
}

// heldProject is one project the way the enricher's store holds it: every
// instance on the project's network, with the address it had when written.
func heldProject(name string, network int, instances map[string]string, config map[string]map[string]string) map[string]any {
	n := testlib.NewNetwork(name, network)
	bridge := iutil.NewNetwork(n.Name, name, true, n.Config["ipv4.address"], "")
	networks := map[string]*iutil.Network{iutil.NetworkKey(name, n.Name): bridge}

	held := map[string]*iutil.Instance{}

	for instance, addr := range instances {
		cfg := map[string]string{}
		for k, v := range config[instance] {
			cfg[k] = v
		}

		held[instance] = iutil.NewInstance(true, cfg, []iutil.InstanceInterface{
			iutil.NewInstanceInterface(name, n.Name, true, []string{addr}, nil),
		}, networks)
	}

	return map[string]any{
		"config":    map[string]string{iutil.FeaturesNetworks: "true"},
		"instances": held,
		"networks":  map[string]*iutil.Network{n.Name: bridge},
	}
}

// restartQuery is one question, and where it comes from.
type restartQuery struct {
	name  string
	qtype uint16

	// from is the address the packet arrived from; empty is loopback.
	from string

	// subnet is the client subnet the query carries; empty carries none.
	subnet string

	// plain sends no OPT record at all.
	plain bool
}

// restartWant is what one question should get.
type restartWant struct {
	rcode int

	// answers is the rdata of the answer section as text, in any order.
	answers []string

	// ttl is what every answer carries; zero leaves it unchecked.
	ttl uint32

	forwarded bool
}

// restartCase is one row of a phase.
type restartCase struct {
	q    restartQuery
	want restartWant
}

// fromAddr is a recorder whose query arrived from a chosen address.
type fromAddr struct {
	recorder

	from net.IP
}

func (f *fromAddr) RemoteAddr() net.Addr { return &net.UDPAddr{IP: f.from, Port: 4242} }

// restartRig is the chain main builds, against a gated Incus and a forwarder
// that answers NXDOMAIN for everything.
type restartRig struct {
	p     *Plugin
	a     *adapter
	ready string

	forwarded int
}

// ask runs one question through the adapter, the way a packet off the wire
// would go, and reports whether it reached the forwarder.
func (r *restartRig) ask(q restartQuery) (*dns.Msg, bool) {
	var m *dns.Msg

	switch {
	case q.subnet != "":
		m = subnetQuery(q.name, q.subnet)
	case !q.plain:
		m = query(q.name)
	default:
		m = new(dns.Msg)
		m.SetQuestion(dns.Fqdn(q.name), dns.TypeA)
	}

	m.Question[0].Qtype = q.qtype

	from := net.IPv4(127, 0, 0, 1)
	if q.from != "" {
		from = net.ParseIP(q.from)
	}

	before := r.forwarded

	w := &fromAddr{from: from}
	r.a.ServeDNS(w, m)

	return w.msg, r.forwarded > before
}

// check asks every row and compares.
func (r *restartRig) check(t *testing.T, phase string, cases []restartCase) {
	t.Helper()

	for _, tc := range cases {
		label := phase + ": " + dns.TypeToString[tc.q.qtype] + " " + tc.q.name
		if tc.q.from != "" {
			label += " from " + tc.q.from
		}

		if tc.q.subnet != "" {
			label += " for " + tc.q.subnet
		}

		if tc.q.plain {
			label += " without EDNS"
		}

		m, forwarded := r.ask(tc.q)
		require.NotNil(t, m, "%s: nothing was written", label)

		assert.Equal(t, dns.RcodeToString[tc.want.rcode], dns.RcodeToString[m.Rcode], label)
		assert.Equal(t, tc.want.forwarded, forwarded, "%s: forwarded", label)

		if forwarded {
			continue
		}

		var got []string

		for _, rr := range m.Answer {
			switch rr := rr.(type) {
			case *dns.A:
				got = append(got, rr.A.String())
			case *dns.PTR:
				got = append(got, rr.Ptr)
			case *dns.CNAME:
				got = append(got, rr.Target)
			}

			if tc.want.ttl != 0 {
				assert.Equal(t, tc.want.ttl, rr.Header().Ttl, "%s: ttl", label)
			}
		}

		assert.ElementsMatch(t, tc.want.answers, got, label)

		if m.Rcode != dns.RcodeServerFailure {
			continue
		}

		// Not Ready rides on a query that brought an OPT record, and on no
		// other: a client that sent none may not parse one.
		if tc.q.plain {
			assert.Nil(t, m.IsEdns0(), "%s: an OPT record the query did not ask for", label)
		} else {
			assert.Equal(t, []uint16{dns.ExtendedErrorCodeNotReady}, edeCodes(m), label)
		}
	}
}

// serial is what a zone's SOA says, asked as a secondary would.
func (r *restartRig) serial(t *testing.T, zone string) uint32 {
	t.Helper()

	m, _ := r.ask(restartQuery{name: zone, qtype: dns.TypeSOA})
	require.NotNil(t, m)
	require.Len(t, m.Answer, 1, "%s has no SOA", zone)

	soa, ok := m.Answer[0].(*dns.SOA)
	require.True(t, ok)

	return soa.Serial
}

// readyz is what /ready answers, as a load balancer asks.
func (r *restartRig) readyz(t *testing.T) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+r.ready+"/ready", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}

	defer func() { _ = resp.Body.Close() }()

	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode == http.StatusOK
}

// freeAddr is a loopback port nothing listens on, for a plugin that takes an
// address rather than a listener.
func freeAddr(t *testing.T) string {
	t.Helper()

	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}

// TestARestartServesTheColdStoreUntilTheFleetIsRead is a restart end to end,
// with the three things it touches at once: the enricher's store replayed, the
// misses failed until warm, and a trusted router answered from every view.
//
// The store holds shop (web, db, both claiming www) and blog (api). Incus says
// nothing until the cold phase has been asked; then it answers that web moved
// and db is gone.
func TestARestartServesTheColdStoreUntilTheFleetIsRead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	www := map[string]map[string]string{
		"web": {userLabel(labelPrefix + metaAliases): "www"},
		"db":  {userLabel(labelPrefix + metaAliases): "www"},
	}

	// What the previous run left: the enricher's fleet and this plugin's serials.
	held := map[string]any{
		"version": 1,
		"projects": map[string]any{
			"shop": heldProject("shop", 0, map[string]string{"web": "10.0.0.5", "db": "10.0.0.6"}, www),
			"blog": heldProject("blog", 1, map[string]string{"api": "10.1.0.5"}, nil),
		},
	}

	b, err := json.Marshal(held)
	require.NoError(t, err)

	store := filepath.Join(dir, "enricher-cold-store.json")
	require.NoError(t, os.WriteFile(store, b, 0o600))

	serials, err := encodeCold(map[string]zoneSerial{"shop.incus.": {Serial: 7}, "blog.incus.": {Serial: 4}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, coldFile), serials, 0o600))

	// What Incus answers once it does.
	gate, conn := newGatedIncus(t,
		liveProject("shop", 0, map[string]string{"web": "10.0.0.7"}, www),
		liveProject("blog", 1, map[string]string{"api": "10.1.0.5"}, nil),
	)

	r := &restartRig{ready: freeAddr(t)}

	upstream := &fakeChain{
		fn: func(w dns.ResponseWriter, q *dns.Msg) {
			r.forwarded++

			m := new(dns.Msg)
			m.SetRcode(q, dns.RcodeNameError)
			_ = w.WriteMsg(m)
		},
		rcode: dns.RcodeNameError,
	}

	enr := enricher.New(slog.Default(),
		enricher.ReadTimeout(time.Minute),
		enricher.StoreFile(store),
	)

	r.p = New(slog.Default(),
		Suffix("incus"),
		TTL(30),
		ColdDir(dir),
		TrustedClients([]netip.Prefix{netip.MustParsePrefix(edgeRouter + "/32")}),
		Chain([]plugin.Plugin{func(plugin.Handler) plugin.Handler { return upstream }}),
	)
	r.a = &adapter{logger: slog.Default(), chain: r.p.xfr}

	h := ihttp.New(slog.Default(), ihttp.Listen(r.ready))

	src, err := source.New(slog.Default(), conn, []iutil.Plugin{enr, r.p, h})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	// Waited for, so nothing still writes into dir when it is removed.
	t.Cleanup(func() {
		gate.release()
		cancel()
		wg.Wait()
	})

	wg.Go(func() { _ = src.Run(ctx) })
	wg.Go(func() { _ = enr.Run(ctx) })
	wg.Go(func() { _ = r.p.Run(ctx) })
	wg.Go(func() { _ = h.Run(ctx) })

	// The replay is folded in windows: wait for every restored name, asked
	// straight at the engine, which needs nothing Run wires. A snapshot seen here
	// was published after the wiring, so the adapter is safe from here on.
	require.Eventually(t, func() bool {
		for _, name := range []string{"web.shop.incus.", "db.shop.incus.", "api.blog.incus."} {
			w := &fromAddr{from: net.ParseIP(edgeRouter)}

			handled, _, err := r.p.view.Answer(t.Context(), w, query(name))
			if err != nil || !handled || w.msg == nil || len(w.msg.Answer) == 0 {
				return false
			}
		}

		return true
	}, 10*time.Second, 10*time.Millisecond, "the cold store was not served while Incus was silent")

	const stale = 5

	r.check(t, "cold", []restartCase{
		// Restored names, to a querier placed by its client subnet: answered,
		// and clamped, since nothing in this run has confirmed them.
		{restartQuery{name: "web.shop.incus.", qtype: dns.TypeA, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeSuccess, answers: []string{"10.0.0.5"}, ttl: stale}},
		{restartQuery{name: "db.shop.incus.", qtype: dns.TypeA, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeSuccess, answers: []string{"10.0.0.6"}, ttl: stale}},
		{restartQuery{name: "5.0.0.10.in-addr.arpa.", qtype: dns.TypePTR, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeSuccess, answers: []string{"web.shop.incus."}, ttl: stale}},

		// Every miss under the suffix may be a name not read yet.
		{restartQuery{name: "nothere.shop.incus.", qtype: dns.TypeA, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeServerFailure}},
		{restartQuery{name: "web.gone.incus.", qtype: dns.TypeA, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeServerFailure}},
		{restartQuery{name: "nothere.shop.incus.", qtype: dns.TypeA, plain: true},
			restartWant{rcode: dns.RcodeServerFailure}},
		{restartQuery{name: "api.blog.incus.", qtype: dns.TypeA, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeServerFailure}},

		// Two claimants of one alias read as missing, and so as not read yet.
		{restartQuery{name: "www.shop.incus.", qtype: dns.TypeCNAME, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeServerFailure}},

		// A reverse zone exists, and a miss in it is unread too.
		{restartQuery{name: "9.0.0.10.in-addr.arpa.", qtype: dns.TypePTR, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeServerFailure}},

		// Outside the suffix nothing is claimed.
		{restartQuery{name: "www.example.com.", qtype: dns.TypeA, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeNameError, forwarded: true}},

		// The router is answered from every view, the other project's included,
		// on the address the packet came from and whatever subnet it relays.
		{restartQuery{name: "api.blog.incus.", qtype: dns.TypeA, from: edgeRouter},
			restartWant{rcode: dns.RcodeSuccess, answers: []string{"10.1.0.5"}, ttl: stale}},
		{restartQuery{name: "web.shop.incus.", qtype: dns.TypeA, from: edgeRouter},
			restartWant{rcode: dns.RcodeSuccess, answers: []string{"10.0.0.5"}, ttl: stale}},
		{restartQuery{name: "api.blog.incus.", qtype: dns.TypeA, from: edgeRouter, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeSuccess, answers: []string{"10.1.0.5"}, ttl: stale}},
		{restartQuery{name: "6.0.0.10.in-addr.arpa.", qtype: dns.TypePTR, from: edgeRouter},
			restartWant{rcode: dns.RcodeSuccess, answers: []string{"db.shop.incus."}, ttl: stale}},

		// And its misses are failed like anybody's.
		{restartQuery{name: "nothere.shop.incus.", qtype: dns.TypeA, from: edgeRouter},
			restartWant{rcode: dns.RcodeServerFailure}},
		{restartQuery{name: "web.gone.incus.", qtype: dns.TypeA, from: edgeRouter},
			restartWant{rcode: dns.RcodeServerFailure}},
		{restartQuery{name: "nothere.shop.incus.", qtype: dns.TypeA, from: edgeRouter, plain: true},
			restartWant{rcode: dns.RcodeServerFailure}},
		{restartQuery{name: "9.0.0.10.in-addr.arpa.", qtype: dns.TypePTR, from: edgeRouter},
			restartWant{rcode: dns.RcodeServerFailure}},
		{restartQuery{name: "www.example.com.", qtype: dns.TypeA, from: edgeRouter},
			restartWant{rcode: dns.RcodeNameError, forwarded: true}},
	})

	assert.Equal(t, uint32(7), r.serial(t, "shop.incus."), "the replay moved a serial")
	assert.Equal(t, uint32(4), r.serial(t, "blog.incus."), "the replay moved a serial")
	assert.False(t, r.p.view.Ready(), "a fleet nothing has read was called ready")
	assert.False(t, r.readyz(t), "/ready before Incus answered anything")

	// Incus answers: web moved, db is gone.
	gate.release()

	require.Eventually(t, func() bool {
		return r.p.view.Ready() && r.readyz(t)
	}, 20*time.Second, 10*time.Millisecond, "the first round never turned the server ready")

	r.check(t, "warm", []restartCase{
		{restartQuery{name: "web.shop.incus.", qtype: dns.TypeA, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeSuccess, answers: []string{"10.0.0.7"}, ttl: 30}},
		{restartQuery{name: "7.0.0.10.in-addr.arpa.", qtype: dns.TypePTR, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeSuccess, answers: []string{"web.shop.incus."}, ttl: 30}},

		// The alias has one claimant left.
		{restartQuery{name: "www.shop.incus.", qtype: dns.TypeCNAME, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeSuccess, answers: []string{"web.shop.incus."}, ttl: 30}},

		// What went, and what was never there, are final now.
		{restartQuery{name: "db.shop.incus.", qtype: dns.TypeA, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeNameError}},
		{restartQuery{name: "5.0.0.10.in-addr.arpa.", qtype: dns.TypePTR, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeNameError}},
		{restartQuery{name: "6.0.0.10.in-addr.arpa.", qtype: dns.TypePTR, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeNameError}},
		{restartQuery{name: "nothere.shop.incus.", qtype: dns.TypeA, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeNameError}},
		{restartQuery{name: "nothere.shop.incus.", qtype: dns.TypeA, plain: true},
			restartWant{rcode: dns.RcodeNameError}},
		{restartQuery{name: "9.0.0.10.in-addr.arpa.", qtype: dns.TypePTR, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeNameError}},
		{restartQuery{name: "api.blog.incus.", qtype: dns.TypeA, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeNameError}},

		// A claimed name in no zone is the forwarder's again.
		{restartQuery{name: "web.gone.incus.", qtype: dns.TypeA, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeNameError, forwarded: true}},
		{restartQuery{name: "www.example.com.", qtype: dns.TypeA, subnet: shopQuerier},
			restartWant{rcode: dns.RcodeNameError, forwarded: true}},

		{restartQuery{name: "api.blog.incus.", qtype: dns.TypeA, from: edgeRouter},
			restartWant{rcode: dns.RcodeSuccess, answers: []string{"10.1.0.5"}, ttl: 30}},
		{restartQuery{name: "web.shop.incus.", qtype: dns.TypeA, from: edgeRouter},
			restartWant{rcode: dns.RcodeSuccess, answers: []string{"10.0.0.7"}, ttl: 30}},
		{restartQuery{name: "db.shop.incus.", qtype: dns.TypeA, from: edgeRouter},
			restartWant{rcode: dns.RcodeNameError}},
		{restartQuery{name: "nothere.shop.incus.", qtype: dns.TypeA, from: edgeRouter},
			restartWant{rcode: dns.RcodeNameError}},
		{restartQuery{name: "web.gone.incus.", qtype: dns.TypeA, from: edgeRouter},
			restartWant{rcode: dns.RcodeNameError, forwarded: true}},
	})

	// What the round corrected is announced once, and only where it moved.
	assert.Equal(t, uint32(8), r.serial(t, "shop.incus."), "the correction was not announced exactly once")
	assert.Equal(t, uint32(4), r.serial(t, "blog.incus."), "a zone that did not move was stepped")
}
