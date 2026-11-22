package enricher

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/coredns/coredns/plugin"
	api "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	icdns "github.com/lxc/incus-compose/cmd/ic-dns/dns"
	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// nxChain is the forwarder behind ic-dns: everything it is asked is NXDOMAIN.
type nxChain struct{}

func (nxChain) Name() string { return "nx" }

func (nxChain) ServeDNS(_ context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	m := new(dns.Msg)
	m.SetRcode(r, dns.RcodeNameError)
	_ = w.WriteMsg(m)

	return dns.RcodeNameError, nil
}

// TestTheNewerRoundHandsOnWhatAnOlderReadFound is the lost read of the sixth
// review end to end, from the enricher's door to an answer from ic-dns.
//
// The run reads for session 2 and a read of the instance is out. The inbox is
// full: the connected of session 3 is held, and the next event of session 3
// goes on dropped, so ic-dns is in session 3 already. The read of session 2
// lands before Run takes the held connected, and goes on stamped 2, which
// ic-dns folds nothing of. Then session 3 opens here and its run reads the
// instance again, with the same answer.
//
// What has to happen: that answer goes on, stamped 3 - the read of session 2
// was not filed as handed on - and ic-dns, fed exactly what the enricher
// handed on and then the end of session 3's round, answers the instance.
func TestTheNewerRoundHandsOnWhatAnOlderReadFound(t *testing.T) {
	p := New(slog.Default(), InboxSize(1))
	require.NoError(t, p.reads.start(1))
	t.Cleanup(p.reads.stop)

	p.reads.readProject = func(_ context.Context, name string) (*api.Project, string, error) {
		return &api.Project{Name: name}, "", nil
	}

	inst := testlib.NewInstance("p", 0, 0)
	state := testlib.NewInstanceState(0, 0)
	p.state.setNetwork(testlib.NewNetwork("p", 0))
	p.reads.flush(t.Context())
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return &inst, state, nil
	}
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstanceWithInterfaces}}

	var handed []*iutil.Event

	p.args.Next = func(ev *iutil.Event) { handed = append(handed, ev) }

	release := func() {
		for ev := range p.q.release() {
			p.args.Next(ev)
		}
	}

	ev := func(action, project, name string, session uint64) *iutil.Event {
		return iutil.NewEvent(time.Now(), action, project, name, "").WithChainState(iutil.ChainCold).WithSession(session)
	}

	// Session 2 up, by hand so no sweeper starts, and its read of the
	// instance out.
	p.held.seen(ev(iutil.ActionConnected, "", "", 2))
	p.sweepSession = 2
	p.accept(t.Context(), ev(api.EventLifecycleInstanceUpdated, "p", inst.Name, 2))
	older := awaitResult(t, p)

	// Session 3 comes up at a full inbox.
	p.Handle(ev("backlog", "", "", 2))
	p.Handle(ev(iutil.ActionConnected, "", "", 3))
	p.Handle(ev(api.EventLifecycleInstanceUpdated, "p", "other", 3))
	require.Len(t, handed, 1, "the second event of session 3 goes on dropped")

	// The read of session 2 lands before Run takes the held connected.
	p.settleRead(t.Context(), older)
	release()

	// Session 3 opens here; its run reads the instance, unstamped as the
	// sweeper sends it.
	<-p.inbox
	require.Len(t, p.held.take(), 1)
	p.sweepSession = 3
	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, ""))
	p.settleRead(t.Context(), awaitResult(t, p))
	release()

	var sessions []uint64

	for _, e := range handed {
		if e.Name() == inst.Name && e.Instance() != nil && e.Err() == nil {
			sessions = append(sessions, e.Session())
		}
	}

	require.Equal(t, []uint64{2, 3}, sessions, "the instance goes on in session 3 after the read of session 2")

	// ic-dns, fed what the enricher handed on, in order.
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := udp.LocalAddr().String()
	require.NoError(t, udp.Close())

	d := icdns.New(slog.Default(),
		icdns.Suffix("example"),
		icdns.TTL(30),
		icdns.Listen(addr),
		icdns.Chain([]plugin.Plugin{func(plugin.Handler) plugin.Handler { return nxChain{} }}),
	)
	require.NoError(t, d.Setup(iutil.SetupArgs{Next: func(*iutil.Event) {}, CommandOut: make(chan iutil.Command, 16)}))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- d.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	// Session 2 had its round, then everything the enricher handed on, then
	// the end of session 3's round as the source mints it.
	d.Handle(ev(iutil.ActionConnected, "", "", 2))
	d.Handle(iutil.NewEvent(time.Now(), iutil.ActionSweepEnd, "", "", "").WithChainState(iutil.ChainWarm).WithSession(2))

	for _, e := range handed {
		d.Handle(e)
	}

	d.Handle(iutil.NewEvent(time.Now(), iutil.ActionSweepEnd, "", "", "").WithChainState(iutil.ChainWarm).WithSession(3))

	name := inst.Name + ".p.example."
	querier := testlib.Address(0, 0)

	query := func() *dns.Msg {
		m := new(dns.Msg)
		m.SetQuestion(name, dns.TypeA)
		m.SetEdns0(1232, false)
		m.IsEdns0().Option = []dns.EDNS0{&dns.EDNS0_SUBNET{
			Code: dns.EDNS0SUBNET, Family: 1, SourceNetmask: 32, Address: net.ParseIP(querier),
		}}

		reply, _, err := (&dns.Client{Net: "udp", Timeout: time.Second}).Exchange(m, addr)
		if err != nil {
			return nil
		}

		return reply
	}

	require.Eventually(t, func() bool {
		reply := query()

		return reply != nil && reply.Rcode == dns.RcodeSuccess && len(reply.Answer) > 0
	}, 10*time.Second, 20*time.Millisecond, "ic-dns does not answer %s after session 3's round", name)

	reply := query()
	require.NotNil(t, reply)

	a, ok := reply.Answer[0].(*dns.A)
	require.True(t, ok, "answer: %v", reply.Answer)
	require.Equal(t, querier, a.A.String())
}
