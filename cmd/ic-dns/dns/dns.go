// Package dns folds events into records and answers queries from them. It owns
// everything it serves, so main knows nothing about DNS.
package dns

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coredns/coredns/plugin"
	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"

	"github.com/lxc/incus-compose/cmd/ic-dns/dns/ecs_view"
	"github.com/lxc/incus-compose/ievent/iutil"
)

// name is what this plugin is called, in the chain and in a drop's reason.
const name = "dns"

// defaultInboxSize is what the inbox holds before a drop is the only answer.
const defaultInboxSize = 1024

// actionRoundLost is what this plugin raises when a sweep-end was dropped, here
// or before here. It walks the chain behind everything already in it and, stamped
// warm, arrives as the ordered signal that sweep-end would have been.
const actionRoundLost = "dns/round-lost"

// publishWindow is how long patches collect before one snapshot is published.
// Trailing edge only, and invisible against a TTL measured in seconds.
const publishWindow = 150 * time.Millisecond

// Config is what this plugin serves and where.
type Config struct {
	// Listen is the address to answer on, UDP and TCP both.
	Listen string

	// Chain is the query chain that answers after the engine, in query order;
	// empty refuses what it does not serve. plugin.Plugin, since CoreDNS wires a
	// stack from closures.
	Chain []plugin.Plugin

	// Stop is what main does about anything in Chain with a lifecycle -
	// forward's health checkers, most of all.
	Stop func()

	// EchoSubnet turns on the RFC 7871 reply option.
	EchoSubnet bool

	// Metrics turns on the engine's counters and gauges.
	Metrics bool

	// ColdAnswers serves what the zones hold before the first round has ended.
	// Off, those queries are SERVFAIL like a cold miss. On unless turned off.
	ColdAnswers bool

	// AllowTransfer is who may ask for a zone transfer. Empty allows nobody, so
	// a transfer is opt-in at the listener as well as at the zone.
	AllowTransfer []netip.Prefix

	// TrustedClients are the queriers answered from every view at once rather
	// than from the one they sit on. Empty trusts nobody, which is the
	// per-querier filter as it stands without this.
	TrustedClients []netip.Prefix
	// PeerViews answers a querier from the networks its own are peered with as
	// well, read off Incus and followed on its network-peer events. Off, which
	// is the default, the peering is neither read nor looked at.
	PeerViews bool

	// ClaimReverse are the prefixes whose reverse zones are ours before any
	// instance in them has been read, the way the suffix is: a cold PTR miss in
	// one of them is SERVFAIL rather than a forwarder's NXDOMAIN. Empty claims
	// no reverse zone, and once warm it changes nothing.
	ClaimReverse []netip.Prefix

	// ColdDir is where the cold store lives, the same directory the certificate
	// is in. Empty disables it, and nothing writes to disk without it.
	ColdDir string

	// ColdSeed is what a previous run published, kept in memory instead of under
	// a directory. Set by ColdMemory, and it wins over ColdDir.
	ColdSeed []byte

	// ColdMemory keeps the store in memory. Its own field because a nil seed is
	// a memory store that starts cold, not the absence of one.
	ColdMemory bool

	// TTL is what a record is rendered with. Short, because a fleet moves and a
	// cached address is one that may have been reassigned.
	TTL uint32

	// Suffix is what a project's zone is built under, unless the project names
	// its own with user.label.dns.zone.
	Suffix string

	// InboxSize defines the backbuffer size.
	InboxSize int

	// Project decides which projects this binary serves, the same predicate the
	// enricher gets. Nil serves every project the certificate can see.
	Project func(p *incusapi.Project) bool
}

// Plugin folds events into a fleet-wide snapshot, publishes it, and answers
// from it. Run's goroutine writes one atomic pointer; every query goroutine reads it.
type Plugin struct {
	cfg    Config
	logger *slog.Logger

	actions []string

	next  iutil.Next
	inbox chan *iutil.Event

	// spill is what the replay could not put on a full inbox, and whatever
	// followed it while it was there, in arrival order; spilled says Run has
	// some to take. A restored event is never dropped: it is a name already
	// on disk, and the enricher replays the whole store in one go before
	// anything reads Incus, so nothing would put a dropped one back until the
	// round. Bounded by the store, and by InboxSize for the live events
	// behind it, which spillLive counts.
	spillMu   sync.Mutex
	spill     []*iutil.Event
	spillLive int
	spilled   chan struct{}

	// commandOut puts an event in at the head of the chain, which is how readiness
	// reaches every position rather than only what comes after this one.
	commandOut chan<- iutil.Command

	// commandIn is the source asking this plugin to finish, on a channel of its own so
	// it arrives whatever the inbox looks like.
	commandIn <-chan iutil.Command

	// view is the engine. It sources nothing and derives nothing: what it holds
	// is whatever was last handed to it here.
	view *ecs_view.ECSView

	// xfr answers transfers, from a snapshot published to it separately. A
	// transfer derives what the query path is built not to, so it reads its own
	// pointer rather than the engine's.
	xfr *xfr

	// ready is what was last announced, so a fold that changes nothing puts no
	// further event on the chain. Run's goroutine alone.
	ready bool

	// cold is what was served last, on disk. This goroutine encodes; the
	// store's own goroutine writes, so a slow disk cannot stall the chain.
	cold *coldStore

	// peerReader reads the peering, on readPeerLoop's goroutine alone. Nil
	// without PeerViews, or with no connection to read it from.
	peerReader peerReader

	// peerAsk asks readPeerLoop for a read of these networks; one slot, so asks
	// coalesce into the newest.
	peerAsk chan peerAsking

	// peerGen numbers the asks; peerNeed is the number of the newest one an
	// event that can move the peering made, and peerLanded that of the newest
	// read that landed. A read asked for before such an event may have been
	// taken before the change it announces, so it does not make the peering
	// known. Run's goroutine alone.
	peerGen, peerNeed, peerLanded uint64

	// peerIn hands a finished read to Run's goroutine, which owns the state.
	peerIn chan peerRead

	// peerCovered is the networks the last read that landed read; nil before
	// one has. Run's goroutine alone.
	peerCovered map[string]bool

	// peersRestored is a peering having come back from the cold store, which
	// counts as known until a read lands. Run's goroutine alone.
	peersRestored bool

	// peerRetry is the backoff after a failed read, first and longest.
	peerRetry [2]time.Duration

	// peerCallTimeout bounds each call of a read of the peering.
	peerCallTimeout time.Duration

	// published is a snapshot having gone out in this run: the replay off disk,
	// or the first round, has been folded and published whole at least once.
	// Run's goroutine alone.
	published bool

	// peersRead is the peering having been read in this run, or not being
	// followed at all. Until it is, no round turns the view warm. Written by
	// Run's goroutine, read by whoever asks PeeringReady.
	peersRead atomic.Bool

	// state is everything folding owns - what is held, what has arrived, and the
	// trees being patched. Run's goroutine alone, never Handle's caller.
	state *state
}

var _ iutil.Plugin = (*Plugin)(nil)

// Option sets one of them. The zero value means unset, and New fills this
// plugin's own default in.
type Option func(*Config)

// InboxSize sets how many events this plugin buffers before it has to drop.
func InboxSize(n int) Option { return func(cfg *Config) { cfg.InboxSize = n } }

// Listen sets the address to answer on, UDP and TCP both. Empty answers no DNS
// at all, which is what a build that only folds asks for.
func Listen(addr string) Option { return func(cfg *Config) { cfg.Listen = addr } }

// Chain sets the query chain that answers after the engine, in the order a
// query travels it. Empty refuses what the engine does not serve.
func Chain(chain []plugin.Plugin) Option { return func(cfg *Config) { cfg.Chain = chain } }

// Stop sets what to do about anything in Chain with a lifecycle of its own.
func Stop(fn func()) Option { return func(cfg *Config) { cfg.Stop = fn } }

// EchoSubnet turns the RFC 7871 reply option on.
func EchoSubnet(v bool) Option { return func(cfg *Config) { cfg.EchoSubnet = v } }

// AllowTransfer sets who may ask for a zone transfer. Empty allows nobody.
func AllowTransfer(prefixes []netip.Prefix) Option {
	return func(cfg *Config) { cfg.AllowTransfer = prefixes }
}

// TrustedClients sets who is answered from every view at once instead of the
// one their address places them on. Empty trusts nobody.
func TrustedClients(prefixes []netip.Prefix) Option {
	return func(cfg *Config) { cfg.TrustedClients = prefixes }
}

// PeerViews answers a querier from the networks its own are peered with too.
func PeerViews(v bool) Option { return func(cfg *Config) { cfg.PeerViews = v } }

// ClaimReverse sets the prefixes whose reverse zones are claimed before warm.
// Empty claims none.
func ClaimReverse(prefixes []netip.Prefix) Option {
	return func(cfg *Config) { cfg.ClaimReverse = prefixes }
}

// Metrics turns the engine's counters and gauges on.
func Metrics(v bool) Option { return func(cfg *Config) { cfg.Metrics = v } }

// ColdAnswers sets whether what the zones hold is served before the first
// round has ended, or SERVFAIL like a cold miss.
func ColdAnswers(v bool) Option { return func(cfg *Config) { cfg.ColdAnswers = v } }

// ColdDir sets where the cold store lives. Empty writes nothing to disk.
func ColdDir(dir string) Option { return func(cfg *Config) { cfg.ColdDir = dir } }

// ColdMemory keeps the cold store in memory, seeded with what a previous run
// published. Nil starts cold, which is a first start.
func ColdMemory(seed []byte) Option {
	return func(cfg *Config) { cfg.ColdSeed, cfg.ColdMemory = seed, true }
}

// TTL sets what a record is rendered with.
func TTL(ttl uint32) Option { return func(cfg *Config) { cfg.TTL = ttl } }

// Suffix sets what a project's zone is built under, unless the project names
// its own.
func Suffix(suffix string) Option { return func(cfg *Config) { cfg.Suffix = suffix } }

// Project sets which projects this binary serves. Nil serves every one the
// certificate can see, which is the standalone default.
func Project(fn func(p *incusapi.Project) bool) Option {
	return func(cfg *Config) { cfg.Project = fn }
}

// New builds the DNS plugin and starts nothing: Run owns every goroutine,
// listener and cold store included.
func New(logger *slog.Logger, opts ...Option) *Plugin {
	cfg := Config{
		InboxSize:   defaultInboxSize,
		ColdAnswers: true,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	logger.Info("Starting", "plugin", name, "config", cfg)

	view := ecs_view.New(logger)
	view.EchoSubnet = cfg.EchoSubnet
	view.Metrics = cfg.Metrics
	view.ColdAnswers = cfg.ColdAnswers
	view.Server = cfg.Listen
	view.Trusted = cfg.TrustedClients

	// Every project zone is built under the suffix, so the suffix is ours before
	// any of them has been read. Empty or "." claims nothing: claiming the root
	// would fail every name on the internet until warm.
	claim := dns.CanonicalName(cfg.Suffix)
	if claim != "." {
		view.Claims = []string{claim}
	}

	// A PTR has no suffix to claim it: the zones of the prefixes named stand in
	// for one, so a restart does not hand a forwarder's NXDOMAIN to a client.
	if reverse := reverseClaims(cfg.ClaimReverse); len(reverse) > 0 {
		view.Claims = append(view.Claims, reverse...)
		logger.Info("Claiming reverse zones", "plugin", name, "zones", reverse)
	}

	view.PeerViews = cfg.PeerViews

	p := &Plugin{
		cfg:       cfg,
		logger:    logger,
		view:      view,
		xfr:       newXFR(logger, cfg.AllowTransfer),
		cold:      newCold(logger, cfg),
		state:     newState(map[string]zoneSerial{}),
		inbox:     make(chan *iutil.Event, cfg.InboxSize),
		spilled:   make(chan struct{}, 1),
		peerAsk:   make(chan peerAsking, 1),
		peerIn:    make(chan peerRead),
		peerRetry: [2]time.Duration{peerRetryFirst, peerRetryLongest},

		peerCallTimeout: peerCallTimeout,
	}

	wants := p.Wants()
	knownActions := make([]string, len(wants))
	for i, w := range wants {
		knownActions[i] = w.Action
	}

	p.actions = knownActions

	// Nothing to wait on where the peering is not followed.
	p.peersRead.Store(!cfg.PeerViews)

	return p
}

// newCold is where this plugin keeps what it published: memory where asked for,
// otherwise the data directory.
func newCold(logger *slog.Logger, cfg Config) *coldStore {
	if cfg.ColdMemory {
		return newMemoryStore(cfg.ColdSeed)
	}

	return newColdStore(logger, cfg.ColdDir)
}

// Name identifies the plugin, and names it in the reason of what it drops.
func (p *Plugin) Name() string { return name }

// Addr is where this plugin answers, for a main that wants to report it.
func (p *Plugin) Addr() string { return p.cfg.Listen }

const wantsInstance = iutil.EnrichedInstance |
	iutil.EnrichedInstanceWithInterfaces |
	iutil.EnrichedProject

// Wants the instance and its networks on anything that moves it, plus what its
// project sets, since a name is built from them. A delete needs no read: the
// name is in the event.
func (p *Plugin) Wants() []iutil.Want {
	// Debounce only where the action repeats. A start or a delete happens once,
	// so collapsing it buys nothing and costs the whole window in latency.
	wants := []iutil.Want{
		{Action: incusapi.EventLifecycleInstanceStarted, Enrich: wantsInstance},
		{Action: incusapi.EventLifecycleInstanceRestarted, Enrich: wantsInstance},
		{Action: incusapi.EventLifecycleInstanceResumed, Enrich: wantsInstance},
		{Action: incusapi.EventLifecycleInstancePaused, Enrich: wantsInstance},
		{Action: incusapi.EventLifecycleInstanceStopped, Enrich: wantsInstance},
		{Action: incusapi.EventLifecycleInstanceShutdown, Enrich: wantsInstance},
		{Action: incusapi.EventLifecycleInstanceUpdated, Enrich: wantsInstance, Debounce: true},
		{Action: incusapi.EventLifecycleInstanceDeleted},
		{Action: incusapi.EventLifecycleInstanceRenamed, Enrich: wantsInstance},
		{Action: incusapi.EventLifecycleNetworkUpdated, Enrich: iutil.EnrichedNetwork, Debounce: true},
	}

	// A peer event only says to read the peering again, so it needs no read.
	// Never collapsed: debounce keys on project and name, and a peer may share
	// its name with an instance whose event it would swallow.
	if p.cfg.PeerViews {
		for _, action := range peerActions {
			wants = append(wants, iutil.Want{Action: action})
		}
	}

	return wants
}

// Setup keeps the successor, the two command doors and the connection - what
// this plugin reads for itself is only which instances still exist.
func (p *Plugin) Setup(args iutil.SetupArgs) error {
	p.next = args.Next
	p.commandIn, p.commandOut = args.CommandIn, args.CommandOut

	// A nil *Connection in the interface would read as a reader.
	if p.cfg.PeerViews && p.peerReader == nil && args.Conn != nil {
		p.peerReader = args.Conn
	}

	return nil
}

// Handle puts the event on the inbox and returns. It runs on the previous
// plugin's goroutine, so a full inbox is a marked drop rather than a wait -
// but for a restored event, which spills instead, and what follows one
// spilled, which goes behind it so nothing overtakes it.
func (p *Plugin) Handle(ev *iutil.Event) {
	p.spillMu.Lock()

	if len(p.spill) == 0 {
		select {
		case p.inbox <- ev:
			p.spillMu.Unlock()

			return
		default:
		}
	}

	if !ev.Restored() && (len(p.spill) == 0 || p.spillLive >= p.cfg.InboxSize) {
		p.spillMu.Unlock()

		p.next(ev.WithDropped(name))
		p.roundLost(ev)

		return
	}

	if !ev.Restored() {
		p.spillLive++
	}

	p.spill = append(p.spill, ev)
	p.spillMu.Unlock()

	select {
	case p.spilled <- struct{}{}:
	default:
	}
}

// foldSpilled folds what spilled, after everything on the inbox: what is
// there arrived before the first of it.
func (p *Plugin) foldSpilled() {
drained:
	for {
		select {
		case ev := <-p.inbox:
			p.fold(ev)
		default:
			break drained
		}
	}

	p.spillMu.Lock()
	spill := p.spill
	p.spill, p.spillLive = nil, 0
	p.spillMu.Unlock()

	for _, ev := range spill {
		p.fold(ev)
	}
}

// roundLost asks for an end of round again when one could not be used, since
// it is what turns the view warm - until then every miss is SERVFAIL - and,
// after a lost stream, healthy and ready again. Its own request counts too, or
// one dropped in turn would end the asking.
// Non-blocking, from either goroutine: a full door drops it like any event.
func (p *Plugin) roundLost(ev *iutil.Event) {
	if ev.Action() != iutil.ActionSweepEnd && ev.Action() != actionRoundLost {
		return
	}

	select {
	case p.commandOut <- iutil.Command{Action: actionRoundLost}:
	default:
	}
}

// Run folds events and answers queries until told to finish. The listener
// outlives the folding: answering from the last snapshot beats refusing.
func (p *Plugin) Run(ctx context.Context) error {
	wire(p.xfr, p.view, p.cfg.Chain)

	if p.cfg.Stop != nil {
		defer p.cfg.Stop()
	}

	// What each zone was published under, so a serial never goes backwards. The
	// records themselves are replayed onto this plugin by the enricher.
	serials, peers := p.cold.load()
	p.state = newState(serials)

	// And the peering they were published under, so the replay writes every
	// peer half as it lands instead of once the peering has been read again,
	// a round of reads per served project later. The read corrects it, and
	// until it has landed what this answers through a peer is as good as the
	// replayed records themselves.
	// An empty one included: a fleet the previous run read with nothing peered
	// is as known as one with peers, and a read failing now keeps it known.
	if p.cfg.PeerViews && peers != nil {
		p.state.setPeers(peers, p.cfg.TTL)
		p.peersRestored = true
		p.logger.Info("restored the network peering from the cold store", "peers", describePeers(peers))
	}

	loopCtx, stopLoop := context.WithCancelCause(ctx)

	// Not ctx, because this is the part that has to outlive the fold loop.
	serveCtx, stopServing := context.WithCancel(context.WithoutCancel(loopCtx))

	var wg sync.WaitGroup

	// Unwinding the other way round: close the store, then stop the listener and
	// wait for both - closing last would drop the last encoding.
	defer wg.Wait()
	defer stopServing()
	defer p.cold.close()
	defer stopLoop(errors.New("end"))

	if p.cold.enabled() {
		wg.Go(p.cold.run)
	}

	// The peering is read off this goroutine, and once before any event asks.
	if p.cfg.PeerViews && p.peerReader != nil {
		wg.Go(func() { p.readPeerLoop(loopCtx) })

		p.askPeers(false)
	} else {
		// No connection to read it over, so nothing a round could wait on.
		p.peersRead.Store(true)
	}

	// An empty address answers no DNS at all, which is what a build that only
	// folds asks for.
	if p.cfg.Listen != "" {
		wg.Go(func() {
			err := serveDNS(serveCtx, p.cfg.Listen, &adapter{logger: p.logger, chain: p.xfr})
			if err != nil {
				stopLoop(fmt.Errorf("answering on %s: %w", p.cfg.Listen, err))
			}
		})
	}

	// The publish window, armed by the first patch of a burst and never reset by
	// the ones after it: a leading edge would step the serial twice per burst.
	var window <-chan time.Time

	for {
		select {
		case <-loopCtx.Done():
			// An abort, not a shutdown: whatever arrived goes nowhere, and the
			// transactions it went into are never committed.
			return nil

		case cmd := <-p.commandIn:
			// Everything already on the inbox, and spilled behind it. Nothing
			// is still feeding either - the source stopped, and the enricher
			// answered its own drain first.
			p.foldSpilled()

			// Before the answer: what the drain collected reaches a snapshot
			// here or nowhere, and nothing would say it was lost.
			p.write()

			// Answered last, so the plugin after this one is asked only once
			// everything held here has been pushed into it.
			select {
			case p.commandOut <- cmd:
			case <-loopCtx.Done():
			}

			return nil

		case <-window:
			window = nil

			p.write()

		case read := <-p.peerIn:
			p.peered(read)

		case ev := <-p.inbox:
			p.fold(ev)

			if window == nil && len(p.state.pending) > 0 {
				window = time.After(publishWindow)
			}

		case <-p.spilled:
			p.foldSpilled()

			if window == nil && len(p.state.pending) > 0 {
				window = time.After(publishWindow)
			}
		}
	}
}

// fold applies one event to what is held, and hands it on. Everything it
// touches belongs to Run's goroutine.
func (p *Plugin) fold(ev *iutil.Event) {
	// Unwinding the other way round: hand the event on, then say what it
	// changed, so the cause reaches the chain ahead of its consequence.
	defer p.announce()

	defer p.next(ev)

	// The peering is read again on anything that can have moved it, and once a
	// round, so an event lost with the stream is caught up on.
	moves := peeringMoves(ev)
	if moves || ev.Action() == iutil.ActionSweepEnd {
		p.askPeers(moves)
	}

	// A peer is not an instance: what it moves arrives with the read.
	if slices.Contains(peerActions, ev.Action()) {
		return
	}

	// A lost stream drops no record, but nothing is confirming them any more,
	// which is what unready says, until a round has ended again. The view stays
	// warm: what it holds is as good as any other server's.
	if ev.Action() == iutil.ActionDisconnected {
		p.view.SetHealthy(false)
		p.state.fresh = false
		p.state.lost = ev.At()
		p.state.down = true

		return
	}

	// Back, after events nobody heard. What a round ended while it was down
	// said is as old as the stream's loss - the source takes commands in its
	// backoff - so the round that counts is the one the enricher starts now.
	// A first connection follows no loss, and takes nothing back.
	if ev.Action() == iutil.ActionConnected {
		if p.state.down {
			p.view.SetHealthy(false)
			p.state.fresh = false
			p.state.lost = ev.At()
			p.state.down = false
		}

		return
	}

	// Only an end that came the whole way in order. One dropped on the way
	// overtook the reads still in flight before it, and rounding on it would
	// turn the view warm over names nobody has read yet. Nor one while the
	// stream is down: the fleet is read again from the top once it is back.
	if ev.Action() == iutil.ActionSweepEnd && ev.Err() == nil {
		if !p.state.down {
			p.rounded()
		}

		return
	}

	if ev.Err() != nil && !p.state.fresh && !p.state.down {
		p.roundLost(ev)
	}

	// The same end arriving another way: anything stamped warm that was not
	// dropped walked the chain behind it, so the round it closed is folded by
	// now even if the sweep-end itself was lost. Stamped before the stream was
	// lost, it is a debounced leftover and says nothing about the stream since.
	// The rule http follows for /ready, so the two agree. Nothing else -
	// a failed read, or a cold stamp after the round - takes it back.
	if !p.state.fresh && !p.state.down && ev.Err() == nil && ev.ChainState() == iutil.ChainWarm && ev.At().After(p.state.lost) {
		p.rounded()
	}

	if ev.Err() != nil || !slices.Contains(p.actions, ev.Action()) {
		return
	}

	// Everything from here keys on the name. Without this, an action carrying
	// none folds into an entry called "" and reaches the cold store.
	if ev.Name() == "" {
		return
	}

	key := heldKey(ev.ProjectName(), ev.Name())

	// A rename is a new name and an old one gone. The event carries both, and
	// the enricher already read the instance under its new name.
	if ev.OldName() != "" {
		p.state.pending[heldKey(ev.ProjectName(), ev.OldName())] = nil
	}

	if ev.Action() == incusapi.EventLifecycleInstanceDeleted {
		p.state.pending[key] = nil

		return
	}

	if !p.serveable(ev) {
		return
	}

	inst := patchInstance(ev, p.cfg.Suffix)
	if inst == nil {
		return
	}

	p.state.pending[key] = inst
}

// rounded takes the end of a round. Healthy is set here, and by the first read
// of the peering where a round ended before it: a replay off disk is
// structurally whole but not fresh, and has to serve clamped until Incus itself
// has been read.
//
// With --peer-views the peering is part of what has to have been read: until
// it has, a name seen only through a peer is missing or stale, and warm would
// answer that querier NXDOMAIN for good instead of SERVFAIL. The round still
// counts as ended, so it is not taken again on every warm event after it.
//
// The window is flushed rather than published beside, or the end of a round
// publishes twice.
func (p *Plugin) rounded() {
	p.state.complete = true

	p.state.write(p.cfg.TTL)
	p.askIfGrew()
	p.publish()
	p.xfr.SetComplete()

	p.state.fresh = true

	if !p.settlePeering() {
		p.logger.Info("a round ended, waiting on a read of the network peering to turn warm")

		return
	}

	p.view.SetHealthy(true)
}

// announce raises dns/ready or dns/not-ready where the view's readiness moved
// since it was last said.
func (p *Plugin) announce() {
	now := p.view.Ready()
	if now == p.ready {
		return
	}

	p.ready = now

	// Ours to name, since nothing else raises or reads it.
	action := "dns/not-ready"
	if now {
		action = "dns/ready"
	}

	// Non-blocking: the source may already have stopped reading, and waiting
	// would hold up the drain this runs inside.
	select {
	case p.commandOut <- iutil.Command{Action: action}:
	default:
	}
}

// PeeringReady is nil once the peering has been read in this run, or where it
// is not followed; otherwise why not. Safe from any goroutine: it is what the
// /ready endpoint asks.
func (p *Plugin) PeeringReady() error {
	if p.peersRead.Load() {
		return nil
	}

	return errPeeringNotRead
}

// errPeeringNotRead is what PeeringReady says until the first read lands.
var errPeeringNotRead = errors.New("the network peering has not been read yet")

// write applies what has arrived and publishes it. Nothing arrived is nothing
// to publish: the window fires once per burst whether or not the burst changed
// anything.
func (p *Plugin) write() {
	if !p.state.write(p.cfg.TTL) {
		return
	}

	p.askIfGrew()
	p.publish()
}

// publish hands the working copy over as current, and stores what it was
// published under.
//
// A serial steps once a round has ended, not on the stamp of the last event:
// a read minted cold before the end and landing after it still changes a zone
// a secondary already holds whole.
func (p *Plugin) publish() {
	p.state.step(p.state.complete, p.cfg.TTL)

	// Written twice, one pointer each: neither reads the other's, and the value
	// itself is finished and shared.
	snap := p.state.snapshot()

	p.view.Replace(snap)
	p.xfr.Replace(snap)

	p.published = true

	p.storeCold()
}

// storeCold hands what a restart needs to the store: the serials published
// under and, where it is followed, the peering. Encoded on this goroutine, so
// what crosses to the writer is a finished []byte and it cannot reach into
// live state.
func (p *Plugin) storeCold() {
	// A run without the flag holds no peering worth restoring.
	var peers map[string][]string
	if p.cfg.PeerViews {
		peers = p.state.peers
	}

	b, err := encodeCold(p.state.serials, peers)
	if err != nil {
		p.logger.Warn("encoding the cold store", "err", err)

		return
	}

	p.cold.store(b)
}

// serveable reports whether this read is one to fold.
func (p *Plugin) serveable(ev *iutil.Event) bool {
	return ev.Enriched(iutil.EnrichedInstanceWithInterfaces)
}

// _ pins the interface here, so a change to it fails the build at the plugin
// rather than at the source that walks it.
var _ iutil.Plugin = (*Plugin)(nil)
