package dns

import (
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/coredns/coredns/plugin"
	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// hostResolved is the one name coldServer's forwarder knows: a cluster node
// under the suffix, which no project zone holds. It has an IPv4 address and no
// IPv6 one, so its AAAA is NODATA, as the uplink's dnsmasq answers it.
const hostResolved = "i1.incus."

// want is what one query should get: an rcode, and whether it reached the
// forwarder to get it.
type want struct {
	rcode     int
	forwarded bool
}

// coldServer is a plugin answering through the adapter, with a forwarder after
// it that answers NXDOMAIN for everything - what a public resolver says about a
// name under a suffix it does not serve - but hostResolved, a cluster node
// sharing the suffix that the uplink's dnsmasq resolves: an A record, and
// NODATA for anything else. Nothing has been folded yet.
type coldServer struct {
	p         *Plugin
	a         *adapter
	forwarded int
}

func newColdServer(t *testing.T, opts ...Option) *coldServer {
	t.Helper()

	c := &coldServer{p: New(slog.Default(), append([]Option{TTL(30)}, opts...)...)}
	c.p.next = func(_ *iutil.Event) {}

	upstream := &fakeChain{
		fn: func(w dns.ResponseWriter, r *dns.Msg) {
			c.forwarded++

			m := new(dns.Msg)
			m.SetRcode(r, dns.RcodeNameError)

			if r.Question[0].Name == hostResolved {
				m.SetReply(r)

				if r.Question[0].Qtype == dns.TypeA {
					m.Answer = []dns.RR{&dns.A{
						Hdr: dns.RR_Header{Name: hostResolved, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
						A:   net.IPv4(10, 0, 4, 1),
					}}
				}
			}

			_ = w.WriteMsg(m)
		},
		rcode: dns.RcodeNameError,
	}

	wire(c.p.xfr, c.p.view, []plugin.Plugin{func(plugin.Handler) plugin.Handler { return upstream }})
	c.a = &adapter{logger: slog.Default(), chain: c.p.xfr}

	return c
}

// check asks every name, as an A or a PTR by its form unless a type comes
// first ("AAAA name"), from an address the fixtures place.
func (c *coldServer) check(t *testing.T, phase string, cases map[string]want) {
	t.Helper()

	for key, want := range cases {
		name := key

		qtype := dns.TypeA
		if dns.IsSubDomain("arpa.", name) {
			qtype = dns.TypePTR
		}

		if typ, rest, ok := strings.Cut(key, " "); ok {
			name, qtype = rest, dns.StringToType[typ]
		}

		r := subnetQuery(name, "10.0.0.2")
		r.Question[0].Qtype = qtype

		before := c.forwarded

		w := &recorder{}
		c.a.ServeDNS(w, r)
		require.NotNil(t, w.msg, "%s: nothing was written for %s", phase, name)

		assert.Equal(t, dns.RcodeToString[want.rcode], dns.RcodeToString[w.msg.Rcode], "%s: %s", phase, name)
		assert.Equal(t, want.forwarded, c.forwarded > before, "%s: was %s forwarded", phase, name)

		if w.msg.Rcode == dns.RcodeServerFailure {
			assert.Equal(t, []uint16{dns.ExtendedErrorCodeNotReady}, edeCodes(w.msg), "%s: %s", phase, name)
		}
	}
}

// TestColdFailsWhatItMayNotHaveReadYet walks a start: one project read and the
// round still going, then the round's end, then the stream dropping. Until the
// round ends a name under the suffix that misses may only be unread, so it is
// SERVFAIL - which sends the client to its next server - rather than whatever
// the forwarder makes of it. Outside every zone read so far the forwarder is
// still asked, and an answer with records is passed on: a cluster node under
// the suffix resolves cold as it does warm, an unread project does not.
func TestColdFailsWhatItMayNotHaveReadYet(t *testing.T) {
	t.Parallel()

	c := newColdServer(t, Suffix("incus"))

	// shop has been read, blog has not.
	c.p.fold(enriched(incusapi.EventLifecycleInstanceStarted, "shop", "web", "10.0.0.2"))
	c.p.write()

	c.check(t, "cold", map[string]want{
		"web.shop.incus.":   {rcode: dns.RcodeSuccess},
		"db.shop.incus.":    {rcode: dns.RcodeServerFailure},
		"api.blog.incus.":   {rcode: dns.RcodeServerFailure, forwarded: true},
		"incus.":            {rcode: dns.RcodeServerFailure, forwarded: true},
		hostResolved:        {rcode: dns.RcodeSuccess, forwarded: true},
		"www.example.com.":  {rcode: dns.RcodeNameError, forwarded: true},
		"shop.incus.":       {rcode: dns.RcodeSuccess},
		"web.shop.incus.x.": {rcode: dns.RcodeNameError, forwarded: true},

		// The node's NODATA says the forwarder holds the name, so it is passed
		// on as well: on musl a SERVFAIL for either half of an A+AAAA pair
		// fails the whole lookup.
		"AAAA " + hostResolved: {rcode: dns.RcodeSuccess, forwarded: true},

		// A reverse zone exists once an address in it has been read, and a
		// miss in it is one more name not read yet. Outside it, nothing claims
		// the address.
		"2.0.0.10.in-addr.arpa.": {rcode: dns.RcodeSuccess},
		"9.0.0.10.in-addr.arpa.": {rcode: dns.RcodeServerFailure},
		"8.8.8.8.in-addr.arpa.":  {rcode: dns.RcodeNameError, forwarded: true},
	})

	// The round's end is what turns the view warm, and from there on every
	// answer is the one a warm server always gave.
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))

	warmCases := map[string]want{
		"web.shop.incus.":        {rcode: dns.RcodeSuccess},
		"db.shop.incus.":         {rcode: dns.RcodeNameError},
		"api.blog.incus.":        {rcode: dns.RcodeNameError, forwarded: true},
		hostResolved:             {rcode: dns.RcodeSuccess, forwarded: true},
		"AAAA " + hostResolved:   {rcode: dns.RcodeSuccess, forwarded: true},
		"www.example.com.":       {rcode: dns.RcodeNameError, forwarded: true},
		"9.0.0.10.in-addr.arpa.": {rcode: dns.RcodeNameError},
		"8.8.8.8.in-addr.arpa.":  {rcode: dns.RcodeNameError, forwarded: true},
	}

	c.check(t, "warm", warmCases)

	// A lost stream leaves the zones as full as they were: it clamps the TTL and
	// nothing else.
	c.p.fold(warm(event(iutil.ActionDisconnected, "", "")))
	require.False(t, c.p.view.Ready())

	c.check(t, "disconnected", warmCases)
}

// TestColdAnswersOffFailsTheHitsToo is the start above with --cold-answers=false:
// before the round ends, a name it already holds - the apex and a PTR included -
// is SERVFAIL like one it does not, so a client with a second server asks that
// one rather than take a record from before the restart. Names outside the
// suffix are forwarded as ever, a name under it in no zone the same way as with
// the switch on, and once warm the switch changes nothing.
func TestColdAnswersOffFailsTheHitsToo(t *testing.T) {
	t.Parallel()

	c := newColdServer(t, Suffix("incus"), ColdAnswers(false))

	c.p.fold(enriched(incusapi.EventLifecycleInstanceStarted, "shop", "web", "10.0.0.2"))
	c.p.write()

	c.check(t, "cold", map[string]want{
		"web.shop.incus.":        {rcode: dns.RcodeServerFailure},
		"shop.incus.":            {rcode: dns.RcodeServerFailure},
		"2.0.0.10.in-addr.arpa.": {rcode: dns.RcodeServerFailure},
		"db.shop.incus.":         {rcode: dns.RcodeServerFailure},
		"api.blog.incus.":        {rcode: dns.RcodeServerFailure, forwarded: true},
		hostResolved:             {rcode: dns.RcodeSuccess, forwarded: true},
		"AAAA " + hostResolved:   {rcode: dns.RcodeSuccess, forwarded: true},
		"www.example.com.":       {rcode: dns.RcodeNameError, forwarded: true},
		"8.8.8.8.in-addr.arpa.":  {rcode: dns.RcodeNameError, forwarded: true},
	})

	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))

	c.check(t, "warm", map[string]want{
		"web.shop.incus.":        {rcode: dns.RcodeSuccess},
		"shop.incus.":            {rcode: dns.RcodeSuccess},
		"2.0.0.10.in-addr.arpa.": {rcode: dns.RcodeSuccess},
		"db.shop.incus.":         {rcode: dns.RcodeNameError},
		"api.blog.incus.":        {rcode: dns.RcodeNameError, forwarded: true},
		hostResolved:             {rcode: dns.RcodeSuccess, forwarded: true},
		"www.example.com.":       {rcode: dns.RcodeNameError, forwarded: true},
	})
}

// TestARootSuffixClaimsNothing pins the guard on the claim: a suffix that is
// the root would fail every name on the internet until warm.
func TestARootSuffixClaimsNothing(t *testing.T) {
	t.Parallel()

	for _, suffix := range []string{"", "."} {
		c := newColdServer(t, Suffix(suffix))

		c.check(t, "suffix "+suffix, map[string]want{
			"www.example.com.": {rcode: dns.RcodeNameError, forwarded: true},
		})
	}
}

// TestALostSweepEndWarmsOnlyInOrder pins the two ways the end of the first round
// can go missing. Neither may leave the view cold for good, and neither may warm
// it early: a sweep-end dropped on the way overtook the reads before it, so only
// something that walked the chain in order behind it may stand in for it.
func TestALostSweepEndWarmsOnlyInOrder(t *testing.T) {
	t.Parallel()

	miss := map[string]want{"db.shop.incus.": {rcode: dns.RcodeServerFailure}}
	answered := map[string]want{"db.shop.incus.": {rcode: dns.RcodeNameError}}

	t.Run("dropped at this plugin's own inbox", func(t *testing.T) {
		t.Parallel()

		c := newColdServer(t, Suffix("incus"), InboxSize(1))

		seen := make(chan *iutil.Event, 4)
		raised := make(chan iutil.Command, 4)
		require.NoError(t, c.p.Setup(iutil.SetupArgs{
			Next:       func(ev *iutil.Event) { seen <- ev },
			CommandOut: raised,
		}))

		// The inbox holds one, so the end of the round behind it is dropped.
		c.p.Handle(enriched(incusapi.EventLifecycleInstanceStarted, "shop", "web", "10.0.0.2"))
		c.p.Handle(warm(event(iutil.ActionSweepEnd, "", "")))

		dropped := <-seen
		require.Equal(t, iutil.ActionSweepEnd, dropped.Action())
		require.Error(t, dropped.Err())

		require.Len(t, raised, 1, "the lost end was not asked for again")
		assert.Equal(t, actionRoundLost, (<-raised).Action)

		c.p.fold(<-c.p.inbox)
		c.p.write()
		c.check(t, "dropped", miss)

		// The request walks the chain and comes back warm, behind everything.
		c.p.fold(warm(event(actionRoundLost, "", "")))
		c.check(t, "asked again", answered)
		assert.True(t, c.p.view.Ready())
	})

	t.Run("dropped before it got here", func(t *testing.T) {
		t.Parallel()

		c := newColdServer(t, Suffix("incus"))

		raised := make(chan iutil.Command, 4)
		c.p.commandOut = raised

		c.p.fold(enriched(incusapi.EventLifecycleInstanceStarted, "shop", "web", "10.0.0.2"))
		c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")).WithDropped("enricher"))
		c.p.write()
		c.check(t, "dropped upstream", miss)

		require.Len(t, raised, 1, "the lost end was not asked for again")
		assert.Equal(t, actionRoundLost, (<-raised).Action)

		// Its own request dropped in turn is asked for again too.
		c.p.fold(warm(event(actionRoundLost, "", "")).WithDropped("enricher"))
		c.check(t, "request dropped", miss)
		require.Len(t, raised, 1)
		<-raised

		// Any event stamped warm that came in order stands in for it.
		c.p.fold(warm(event(incusapi.EventLifecycleInstanceUpdated, "shop", "web")))
		c.check(t, "a warm event in order", answered)
	})

	t.Run("a warm leftover from before the stream went", func(t *testing.T) {
		t.Parallel()

		c := newColdServer(t, Suffix("incus"))

		stale := warm(event(incusapi.EventLifecycleInstanceUpdated, "shop", "web"))

		c.p.fold(enriched(incusapi.EventLifecycleInstanceStarted, "shop", "web", "10.0.0.2"))
		c.p.fold(iutil.NewEvent(stale.At().Add(time.Second), iutil.ActionDisconnected, "", "", "").
			WithChainState(iutil.ChainCold))
		c.p.fold(stale)
		c.p.write()

		c.check(t, "stale", miss)
		assert.False(t, c.p.view.Ready(), "healthy while the stream is down")
	})
}

// edeCodes is every RFC 8914 info code a reply carries.
func edeCodes(m *dns.Msg) []uint16 {
	opt := m.IsEdns0()
	if opt == nil {
		return nil
	}

	var out []uint16

	for _, o := range opt.Option {
		ede, ok := o.(*dns.EDNS0_EDE)
		if ok {
			out = append(out, ede.InfoCode)
		}
	}

	return out
}
