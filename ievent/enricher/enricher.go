// Package enricher reads what an event's subject looks like now, and fills the
// rest of the event in from what it already holds. See
// docs/root/developer/ievent/enricher.md for the design.
package enricher

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	incusapi "github.com/lxc/incus/v7/shared/api"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// name is what this plugin is called, in the chain and in failure reasons.
const name = "enricher"

// Defaults, used for whatever main left unset.
const (
	defaultWorkers = 16

	defaultReadTimeout = 10 * time.Second

	// defaultReadDelay is the gap between the reads inside one project.
	defaultReadDelay = 5 * time.Second

	defaultSweepInterval = 6 * time.Hour

	// defaultSweepRetry and defaultSweepRetryMax bound the wait after a run
	// that broke: a second, doubling, never more than half a minute.
	defaultSweepRetry    = time.Second
	defaultSweepRetryMax = 30 * time.Second

	// defaultStoreInterval is how often what this plugin holds is written, and
	// so how much of a fleet a start finds stale.
	defaultStoreInterval = 5 * time.Second

	// defaultIPTimeout bounds how long a worker polls for an instance to acquire an
	// IP address when interfaces are requested.
	defaultIPTimeout = 10 * time.Second

	// defaultRoundGrace is how long the end of a run waits on instances whose
	// read failed: several retries at the default read timeout, so a daemon
	// slow for a while is waited out, and one failing a read for good holds
	// the chain cold for no longer than this.
	defaultRoundGrace = time.Minute
)

// storeStopTimeout bounds the wait for the last write on the way down. A disk
// that will not answer costs the cold store, never the shutdown.
const storeStopTimeout = 5 * time.Second

// retryDelay/retryTries/slowRetryDelay: how soon and how often a failed key
// is read again, and the slower rate it drops to once retryTries is spent.
const (
	retryDelay     = time.Second
	retryTries     = 3
	slowRetryDelay = 5 * time.Second
)

// defaultInboxSize holds a burst before Handle has to drop; matched to the
// Incus client's own event channel size.
const defaultInboxSize = 1024

// wantsInstanceRead is either way a plugin asks for an instance to be read.
// There are no interfaces without the instance they belong to, so naming only
// the second still asks for one - a plugin that asked for enrichment and got
// none silently would serve an empty fleet and never say why.
const wantsInstanceRead = iutil.EnrichedInstance | iutil.EnrichedInstanceWithInterfaces

// instancePrefix is how an event that names an instance is told from one that
// names a network with the same bare name.
const instancePrefix = "instance-"

// profilePrefix is what an action names a profile with.
const profilePrefix = "profile-"

// The resource a key names. Three characters, hand-picked rather than cut off
// the action: every Incus entity is longer than that, but profile and project
// share their first three.
const (
	kindInstance = "ins"
	kindNetwork  = "net"
	kindProject  = "prj"
)

// errRead is why an event fails: the read it was waiting on did not land.
var errRead = errors.New("enricher/read")

// poolDelay is how long a submit the nonblocking pool refused waits before
// being offered again; nothing is failed for the pool being busy.
const poolDelay = 20 * time.Millisecond

// Plugin fills events in from what it holds, reading only the subject of each.
//
// Everything below the inbox belongs to the goroutine Run owns; a pool worker
// touches nothing here, which is what keeps this package free of a mutex.
type Plugin struct {
	logger *slog.Logger

	args iutil.SetupArgs
	opts options

	inbox chan *iutil.Event

	// sweeper reads the fleet on a goroutine of its own; sweeps is the channel
	// it shares with this one, and sweeperCancel is how a run is abandoned.
	sweeperCancel context.CancelFunc
	sweeps        chan sweepMsg

	// sweepRun is the run the sweeper now running was started as; what an
	// abandoned one still says is not listened to.
	sweepRun uint64

	// sweepSession is the stream session the run now running was started
	// for: the connected that restarted it. Its end names it, so the source
	// takes that end only while the same stream is up. 0, the run Run starts
	// before any stream opened, is never taken.
	sweepSession uint64

	// excluded is the projects the last listing of projects found and the
	// configuration does not serve. See settleRead.
	excluded map[string]bool

	// closedSession is the newest session a disconnected closed. See
	// sessionOf.
	closedSession uint64

	// held is what the inbox had no room for and may not be dropped, kept by
	// session. See Handle.
	held *heldSessions

	// sweepEnding says a run reached its end while its project reads were still
	// out, or instances it listed were unread, so whatever settles the last of
	// them announces it instead.
	sweepEnding bool

	// unread is every instance a run listed, or whose read failed, that
	// nothing holds and no read has landed for: a name nobody downstream has,
	// so a run ending before it lands would make the chain warm - the end's
	// command stamps everything after it warm at once - and the name's
	// absence final. Keyed like a call.
	unread map[string]subject

	// brokenSince is when the first of the runs breaking since the last one
	// that ended broke; zero while none is. See endBroken.
	brokenSince time.Time

	// readAt is when the read each held instance was last answered by went
	// out: a listing asked for before it may not have seen an instance made
	// since, and is not taken as saying it went.
	readAt map[subject]time.Time

	// netMoved is when an event last arrived about each network, by project
	// and name: a read or a listing of it asked for before then is older news
	// than the event, and is not taken. A listing of the project's networks
	// asked for after it settles it (settleNetworkMoves).
	netMoved map[string]map[string]time.Time

	// owedDelete is every instance a read found gone while a later read of it
	// was owed, keyed like a call: nothing holds it any more, so the delete is
	// the key's to announce - by the next read that lands and finds it gone,
	// or the next listing that leaves it out - and not lost with a read that
	// fails meanwhile.
	owedDelete map[string]owedDelete

	// endGrace is when a run held on unread ends anyway, and graceOver says it
	// has come: an instance whose read fails for good holds a run up to this
	// and no further.
	endGrace  <-chan time.Time
	graceOver bool

	// storeWrite is where a clone of the state is put. A field rather than the
	// path, so a test answers without a disk.
	storeWrite writeFunc

	// The store writes on a goroutine of its own. All three are nil where
	// nothing is written, and a nil channel is one a select never picks.
	storeIn   chan *state
	storeDone chan struct{}
	storeTick <-chan time.Time

	// Everything below belongs to the goroutine Run owns.
	q       *queue
	state   *state
	reads   *deferred
	retries *retries
	archive archive

	// chain is what the last event said, and what this plugin stamps on the ones
	// it creates: those never pass the source, so nothing else would.
	chain iutil.ChainState
}

var _ iutil.Plugin = (*Plugin)(nil)

// options is what main decides about this plugin, kept separate from a shared
// iutil options type since nothing here has a debounce window.
type options struct {
	Workers       int
	ReadTimeout   time.Duration
	IPTimeout     time.Duration
	InboxSize     int
	ReadDelay     time.Duration
	SweepInterval time.Duration
	StoreInterval time.Duration

	// sweepRetry and sweepRetryMax are the backoff after a run that broke.
	// Unexported: a test shortens them, nothing else has a reason to.
	sweepRetry    time.Duration
	sweepRetryMax time.Duration

	// roundGrace is how long a run's end waits on failed instance reads.
	// Unexported for the same reason.
	roundGrace time.Duration

	TTL       time.Duration
	Project   func(p *incusapi.Project) bool
	Instance  func(i *incusapi.Instance) bool
	StoreFile string
	Metrics   bool
}

// Option sets one of the options; New fills in defaults for whatever is left
// zero.
type Option func(*options)

// Workers caps the Incus reads this plugin may have in flight. One endpoint
// fronts a whole cluster, so this bounds load on somebody else's machine.
func Workers(n int) Option { return func(o *options) { o.Workers = n } }

// ReadTimeout bounds one read of the daemon.
func ReadTimeout(d time.Duration) Option { return func(o *options) { o.ReadTimeout = d } }

// IPTimeout bounds how long a worker polls for an instance to acquire an IP
// address when interfaces are requested.
func IPTimeout(d time.Duration) Option { return func(o *options) { o.IPTimeout = d } }

// InboxSize sets how many events this plugin buffers before it has to drop.
func InboxSize(n int) Option { return func(o *options) { o.InboxSize = n } }

// ReadDelay sets the gap between the reads inside one project.
func ReadDelay(d time.Duration) Option { return func(o *options) { o.ReadDelay = d } }

// SweepInterval sets the duration to wait between sweeps.
func SweepInterval(d time.Duration) Option { return func(o *options) { o.SweepInterval = d } }

// TTL is how long what is held stays valid. Any read revalidates it, whoever
// asked - a run, an event and a re-read all move the same clock - and a key
// still held when it expires is read again. Zero leaves the run as the only
// thing that comes back round.
//
// Set it below the time a run takes on the fleet and every key expires before
// the run reaches it, so the pool carries the whole fleet instead.
func TTL(d time.Duration) Option { return func(o *options) { o.TTL = d } }

// Project sets which projects the binary serves. Nil serves every one the
// certificate can see, which is the standalone default:
//
//	enricher.Project(func(p *incusapi.Project) bool {
//		return p.Config["user.dns"] == "true"
//	})
func Project(fn func(p *incusapi.Project) bool) Option {
	return func(o *options) { o.Project = fn }
}

// Instance sets which instances the binary serves. Nil serves every instance in
// a served project:
//
//	enricher.Instance(func(i *incusapi.Instance) bool {
//		return i.Config["user.label.caddy"] != ""
//	})
func Instance(fn func(i *incusapi.Instance) bool) Option {
	return func(o *options) { o.Instance = fn }
}

// StoreFile is the file path to the cold store file, use an empty one to disable.
func StoreFile(f string) Option { return func(o *options) { o.StoreFile = f } }

// StoreInterval is how often what this plugin holds is written, where StoreFile
// says to write it. A run that changed nothing writes nothing whatever this is.
func StoreInterval(d time.Duration) Option { return func(o *options) { o.StoreInterval = d } }

// Metrics turns counters and gauges on.
func Metrics(v bool) Option { return func(o *options) { o.Metrics = v } }

// New builds an enricher.
//
// ReadTimeout starts when a worker picks a read up, not when it is offered to
// the pool, so time spent waiting for a worker is never charged to the daemon.
func New(logger *slog.Logger, opts ...Option) *Plugin {
	o := options{
		Workers:       defaultWorkers,
		ReadTimeout:   defaultReadTimeout,
		IPTimeout:     defaultIPTimeout,
		InboxSize:     defaultInboxSize,
		ReadDelay:     defaultReadDelay,
		SweepInterval: defaultSweepInterval,
		StoreInterval: defaultStoreInterval,
		sweepRetry:    defaultSweepRetry,
		sweepRetryMax: defaultSweepRetryMax,
		roundGrace:    defaultRoundGrace,
	}

	for _, opt := range opts {
		opt(&o)
	}

	logger.Info("Starting", "plugin", name, "config", o)

	// Unbuffered, so the sweeper's pace is felt rather than run ahead of.
	sweeps := make(chan sweepMsg)

	p := &Plugin{
		logger: logger,
		opts:   o,
		inbox:  make(chan *iutil.Event, o.InboxSize),
		held:   newHeldSessions(),
		sweeps: sweeps,

		q:          &queue{},
		unread:     map[string]subject{},
		owedDelete: map[string]owedDelete{},
		netMoved:   map[string]map[string]time.Time{},
		readAt:     map[subject]time.Time{},
		state:      loadState(logger, o.StoreFile, o.Project),
		reads:      newDeferred(o.Workers, o.ReadTimeout),
		retries:    newRetries(),
		archive:    archive{},
	}

	// Nil where nothing is written, which is what the fold and the shutdown
	// both check rather than the path.
	if o.StoreFile != "" {
		p.storeIn = make(chan *state, 1)
		p.storeDone = make(chan struct{})
	}

	return p
}

// storeArgs is what the store goroutine is given.
func (p *Plugin) storeArgs() storeArgs {
	return storeArgs{write: p.storeWrite, in: p.storeIn, done: p.storeDone}
}

// storeClone hands the store what this plugin holds, where anything has changed
// since the last one. On the timer, and once on the way down.
func (p *Plugin) storeClone() {
	if p.storeIn == nil || !p.state.dirty {
		return
	}

	p.state.dirty = false

	storeSend(p.storeIn, p.state.clone())
}

func (p *Plugin) updateMetrics() {
	if !p.opts.Metrics {
		return
	}

	projectsGauge.Set(float64(p.state.projectCount()))
	instancesGauge.Set(float64(p.state.instanceCount()))
}

// Name identifies the plugin, and names it in the reason of what it fails.
func (p *Plugin) Name() string { return name }

// Wants the actions it acts on itself, and no read for any of them: it must
// read SetupArgs.Wanted instead, or it would double-count everyone else's.
func (p *Plugin) Wants() []iutil.Want {
	return []iutil.Want{
		// Collapsible: a profile carries no history an update could lose.
		{Action: incusapi.EventLifecycleProfileUpdated, Debounce: true},

		// Not collapsed: a delete lost in a burst is a delete lost.
		{Action: incusapi.EventLifecycleProfileDeleted},

		// A bridge raises no update after it is created, so without this a NIC
		// on a new one stays unplaced until the next sweep.
		{Action: incusapi.EventLifecycleNetworkCreated},

		// Not collapsed: a delete or rename lost in a burst leaves the network
		// in the state, and the instances on it filed under a key nothing
		// describes.
		{Action: incusapi.EventLifecycleNetworkDeleted},
		{Action: incusapi.EventLifecycleNetworkRenamed},
	}
}

// Setup keeps what it was handed and starts nothing; the goroutine is Run's.
func (p *Plugin) Setup(args iutil.SetupArgs) error {
	p.args = args

	if p.reads.read == nil {
		p.reads.read = incusReader(p.logger, args.Conn, p.opts.IPTimeout, p.opts.Instance)
	}

	if p.reads.readNet == nil {
		p.reads.readNet = args.Conn.GetNetwork
	}

	if p.reads.readProject == nil {
		p.reads.readProject = args.Conn.GetProject
	}

	if p.reads.fleet == nil {
		p.reads.fleet = args.Conn
	}

	if p.storeWrite == nil && p.opts.StoreFile != "" {
		p.storeWrite = fileWriter(p.opts.StoreFile)
	}

	return nil
}

// Handle puts the event on the inbox and returns.
//
// It runs on the previous plugin's goroutine and must not block, so a full
// inbox drops rather than waits; dropped, not failed, since nothing went wrong.
//
// But for a connection control, and for the first event of a session newer
// than any seen here: a connected is what restarts the run for its session,
// a disconnected handed on dropped would overtake what is queued, and the
// first news of a session dropped would leave the run reading for the one
// before. They are held by session (heldSessions), for Run to take as if they
// had come through the inbox; one of a session older than the newest seen
// here is a leftover, and a control that is goes nowhere.
func (p *Plugin) Handle(ev *iutil.Event) {
	newer := p.held.seen(ev)

	select {
	case p.inbox <- ev:
		return
	default:
	}

	control := ev.Action() == iutil.ActionConnected || ev.Action() == iutil.ActionDisconnected

	if (control || newer) && p.held.keep(ev) {
		return
	}

	if control {
		return
	}

	p.args.Next(ev.WithDropped(name))
}

// Run enriches until ctx is canceled. It blocks, so main owns the goroutine:
//
//	wg.Go(func() error { return enr.Run(ctx) })
//
// Reads in flight are abandoned rather than waited for; the events themselves
// still go.
func (p *Plugin) Run(ctx context.Context) error {
	err := p.reads.start(p.opts.Workers)
	if err != nil {
		return err
	}

	defer p.reads.stop()
	defer p.retries.stop()

	// Its own context, so the run stops when Run returns, not only when the
	// process does.
	sweepCtx, stopSweeping := context.WithCancel(ctx)
	p.sweeperCancel = stopSweeping
	defer func() {
		if p.sweeperCancel != nil {
			p.sweeperCancel()
		}
	}()

	// Ahead of the first run, which is the point: what the last run of this
	// process held answers until this one has read it again, rather than
	// nothing answering until it has.
	p.replay()

	runSweeper(sweepCtx, p.logger, p.sweepArgs(), p.opts.SweepInterval)

	if p.storeIn != nil {
		ticker := time.NewTicker(p.opts.StoreInterval)
		defer ticker.Stop()

		p.storeTick = ticker.C

		// Its own context, not this one: a clone offered on the way down still
		// has to be written, and an abort cancels ctx before that happens.
		storeCtx, stopStore := context.WithCancel(context.Background())

		defer func() {
			p.storeClone()
			stopStore()

			select {
			case <-p.storeDone:
			case <-time.After(storeStopTimeout):
				p.logger.Warn("the fleet was still being written when this stopped waiting",
					"plugin", name)
			}
		}()

		runStore(storeCtx, p.logger, p.storeArgs())
	}

	drain := func(cmd iutil.Command) {
	DRAINED:
		for {
			select {
			case ev := <-p.inbox:
				p.accept(ctx, ev)
			default:
				break DRAINED
			}
		}

		for _, ev := range p.held.take() {
			p.accept(ctx, ev)
		}

		// Then the whole line, settled or not, in arrival order. Reads
		// still in flight are abandoned rather than waited for.
		for ev := range p.q.drain() {
			p.args.Next(ev)
		}

		// Answered last, so the plugin after this one is asked only once
		// everything this one held has been pushed into it.
		select {
		case p.args.CommandOut <- cmd:
		case <-ctx.Done():
		}
	}

	for {
		select {
		case <-ctx.Done():
			// An abort, not a shutdown: reads in flight are abandoned.
			return nil

		case cmd := <-p.args.CommandIn:
			// Everything already on the inbox; nothing feeds it further once the
			// source has stopped.
			drain(cmd)
			return nil

		case ev := <-p.inbox:
			p.accept(ctx, ev)

			for ev := range p.q.release() {
				p.args.Next(ev)
			}

		case <-p.held.ready:
			// Behind what the inbox held when it had no room for it.
		queued:
			for {
				select {
				case held := <-p.inbox:
					p.accept(ctx, held)
				default:
					break queued
				}
			}

			for _, ev := range p.held.take() {
				p.accept(ctx, ev)
			}

			for ev := range p.q.release() {
				p.args.Next(ev)
			}

		case msg := <-p.sweeps:
			p.acceptSweep(ctx, msg)

		case res := <-p.reads.results:
			// The patch happens here, not in the worker: the state belongs to
			// this goroutine.
			p.settleRead(ctx, res)

			for ev := range p.q.release() {
				p.args.Next(ev)
			}

		case <-p.retries.timer.C:
			// Through the fan-out: a re-read has to make an event of its own,
			// or what it finds reaches nobody.
			p.fanOut(ctx, p.retries.take(time.Now()))

		case <-p.storeTick:
			p.storeClone()

		case <-p.endGrace:
			p.endGrace = nil
			p.graceOver = true

			p.logger.Warn("ending the run with instances whose read keeps failing",
				"plugin", name, "instances", len(p.unread))

			p.endIfRead(ctx)

		case <-p.reads.timer.C:
			p.reads.retry(ctx)
		}
	}
}

// replay hands on every instance the cold store restored, each as the
// instance-updated a read of it would have made, marked restored and stamped
// cold: nothing in this run has confirmed any of it.
//
// Filed in the archive like any event emitted here, so the run's read of an
// instance that has not changed is news once - it is the first fresh answer,
// and a consumer waiting on one has to see it - and then no more. What has
// changed, or gone, the run corrects the way it would anything else.
func (p *Plugin) replay() {
	want := p.args.Wanted[incusapi.EventLifecycleInstanceUpdated].Enrich
	if want&wantsInstanceRead == 0 {
		return
	}

	for _, s := range p.state.restored() {
		// Whole, the way settleRead hands on every read that lands, or the run's
		// read of an unchanged instance would differ from this by more than
		// where it came from.
		ev := p.withProject(iutil.NewEvent(time.Now(),
			incusapi.EventLifecycleInstanceUpdated, s.project, s.instance, "").
			WithChainState(iutil.ChainCold).
			WithInstance(p.state.instance(s.project, s.instance), true).
			WithRestored())

		p.archive.changed(ev)
		p.args.Next(ev)
	}

	p.updateMetrics()
}

// withProject attaches the project's own configuration out of the state, not a
// read of its own, where somebody asked for it.
//
// A project no run has reached is left unenriched rather than enriched with
// nothing, so a consumer can tell "sets none" from "not read yet".
func (p *Plugin) withProject(ev *iutil.Event) *iutil.Event {
	if p.args.Wanted[ev.Action()].Enrich&iutil.EnrichedProject == 0 {
		return ev
	}

	config := p.state.projectConfig(ev.ProjectName())
	if config == nil {
		return ev
	}

	return ev.WithProject(iutil.NewProject(config))
}

// readProject reads a project nothing has read yet.
//
// The event goes on unenriched: what lands answers the next one about that
// project. Coalesced by key, so a burst naming one project costs one read, and
// one a run has already asked for costs none.
func (p *Plugin) readProject(ctx context.Context, ev *iutil.Event) {
	if p.args.Wanted[ev.Action()].Enrich&iutil.EnrichedProject == 0 {
		return
	}

	project := ev.ProjectName()
	if project == "" || p.state.projectConfig(project) != nil {
		return
	}

	p.reads.send(ctx, &call{
		key:     resourceKey(kindProject, project, ""),
		kind:    kindProject,
		project: project,
	})
}

// readNetwork reads a network a NIC named and nothing has read.
//
// Asked for in the instance's own project: a project without features.networks
// references the default project's, and Incus answers with the project on it, so
// one read resolves either shape.
func (p *Plugin) readNetwork(ctx context.Context, project, name string) {
	if name == "" {
		return
	}

	p.reads.send(ctx, &call{
		key:     resourceKey(kindNetwork, project, name),
		kind:    kindNetwork,
		project: project,
		name:    name,
	})
}

// settleRead patches the state from one read and settles whatever was waiting
// on it. It runs on the goroutine Run owns, because the state does.
func (p *Plugin) settleRead(ctx context.Context, res result) {
	c := res.call
	if !p.reads.current(c) {
		return
	}

	p.reads.done(ctx, c)

	switch c.kind {
	case kindNetwork:
		if res.err != nil {
			// Left to the run: a re-read here would ask the daemon for an
			// instance by this name, since the fan-out only makes those.
			return
		}

		// Older than an event about the network, under the key it was asked
		// for or the one Incus answered with - a tenant's NIC naming the
		// default project's network reads it under the tenant - it would undo
		// that event: the event's own read says what the network is now.
		if p.networkMoved(c.project, c.name, c.sent) || p.networkMoved(res.network.Project, res.network.Name, c.sent) {
			return
		}

		// The network first, so the re-reads resolve against it, not what it
		// replaced. Keyed by the project that owns it rather than the one that
		// named it, which for a bridge may be the default project.
		p.state.setNetwork(*res.network)
		p.fanOut(ctx, p.state.networkInstances(res.network.Project, res.network.Name))

		return

	case kindProject:
		if res.err == nil {
			p.state.setProject(c.project, res.project.Config)
			p.updateMetrics()
			p.fanOut(ctx, p.state.projectInstances(c.project))
		}

		// A project that would not answer still counts: the run is over either
		// way, and holding warm on it would leave the chain cold for ever.
		p.endIfRead(ctx)

		return
	}

	// In a project the listing of projects found and does not serve: the
	// prune took everything in it, and a read that went out before it would
	// put an instance back that nothing serves any more.
	if p.excluded[c.project] {
		for _, it := range c.items {
			p.q.trash(it)
		}

		delete(p.unread, c.key)
		p.endIfRead(ctx)

		return
	}

	// A read whose answer would go on only in a session older than the newest
	// there is news of: no fan-out of its own, which goes on stamped as of
	// now, and every event it holds a leftover by now. The plugins after this
	// one fold nothing of it, so nothing here takes it in either - not the
	// state, which a listing is compared against, nor what the end of the
	// round waits on (isCurrent). Its events go on with what it found, held
	// nowhere: the round of the newer session reads the instance for itself,
	// and finds it neither held nor archived. A listing's verdict that waited
	// on it stands.
	if c.ev == nil && len(c.items) > 0 && !slices.ContainsFunc(c.items, func(it *item) bool { return p.isCurrent(it.ev) }) {
		var found *iutil.Instance
		if res.err == nil && res.instance != nil {
			found, _ = p.state.buildInstance(res.instance, res.state)
		}

		for _, it := range c.items {
			ev := it.ev
			if found != nil {
				ev = p.withProject(ev.WithInstance(found, true))
			}

			p.q.settle(it, ev)
		}

		if verdict, ok := p.owedDelete[c.key]; ok && !verdict.gone.IsZero() && p.reads.calls[c.key] == nil {
			p.accept(ctx, iutil.NewEvent(time.Now(), incusapi.EventLifecycleInstanceDeleted, c.project, c.name, ""))
		}

		p.endIfRead(ctx)

		return
	}

	// Found by a read that went out before a listing that left the instance
	// out: the answer may well be older than the delete the listing saw, the
	// delete itself lost with the stream. It does not decide alone. A read of
	// the key already asked for after it decides; with none, the listing's
	// verdict is final (listedGone).
	if verdict, ok := p.owedDelete[c.key]; ok && !verdict.gone.IsZero() && !c.sent.After(verdict.gone) && res.err == nil && res.instance != nil {
		for _, it := range c.items {
			p.q.settle(it, it.ev.WithFailed(errRead))
		}

		if p.reads.calls[c.key] == nil {
			p.accept(ctx, iutil.NewEvent(time.Now(), incusapi.EventLifecycleInstanceDeleted, c.project, c.name, ""))
		}

		p.endIfRead(ctx)

		return
	}

	_, owed := p.owedDelete[c.key]
	had := p.state.instance(c.project, c.name) != nil || owed
	unserved := p.opts.Instance != nil && res.instance != nil && !p.opts.Instance(res.instance)

	inst, landed := p.patchState(ctx, c.project, c.name, res.instance, res.state, res.err)
	if landed {
		p.updateMetrics()
	}

	// A held instance whose read failed is still answered from what is held;
	// one nothing holds is a name the run has not read yet. It is read again
	// (patchState scheduled that), and the end of a run waits on it.
	switch {
	case landed:
		delete(p.unread, c.key)
	case p.state.instance(c.project, c.name) == nil:
		p.unread[c.key] = subject{project: c.project, instance: c.name}
	}

	defer p.endIfRead(ctx)

	// Before the event it made below, which is then compared against what they
	// filed.
	for _, it := range c.items {
		if !landed {
			// Not filed: the next read compares against the last answer there
			// was, not one that did not land.
			p.q.settle(it, it.ev.WithFailed(errRead))

			continue
		}

		if unserved && !had {
			p.q.trash(it)

			continue
		}

		ev := it.ev
		if inst != nil {
			// Merged after the read, not before: the instance's own side of it
			// is what the read just brought back.
			ev = p.withProject(ev.WithInstance(inst, true))
		}

		// Of a session older than the newest by now: what comes after this
		// plugin folds nothing of it, so the archive does not take it as
		// handed on either, or the next read of the same answer would be
		// held back as nothing new.
		if !p.isCurrent(ev) {
			p.q.settle(it, ev)

			continue
		}

		if !p.archive.changed(ev) {
			p.q.trash(it)

			continue
		}

		p.q.settle(it, ev)
	}

	// Through withProject like every other instance event emitted here, or it is
	// not the same event a live one on this key is.
	//
	// Stamped again on the way out: the event was minted when the read was
	// asked for, and a fan-out of a newer session joined to it, or a session
	// come up while it was out, would leave it stamped older than what it
	// says, which is news as of now.
	if c.ev != nil && inst != nil {
		syn := p.withProject(p.stamp(c.ev).WithInstance(inst, true))
		if p.archive.changed(syn) {
			p.q.push(syn, true)
		}
	}

	// Held before this read and not after it: Incus no longer has it, or no
	// longer serves it. A live delete may be on its way with the same news, but
	// one that happened while the stream was down - or this process was, and
	// the cold store put it back - is not, and a fan-out reaching it first
	// leaves the listing nothing to prune. Announced here, then, the way a
	// prune does; a second delete for the same name changes nothing.
	//
	// Unless a read of the same key is already owed: it was asked for after
	// this one went out, so its events are in the line ahead of where this
	// delete would go, and an instance made again under the name in between
	// would reach the chain and then be taken away by the older news. The
	// delete is owed by the key instead, not by that read: the next read of
	// it that lands and finds it gone announces it, however many fail first,
	// and so does a listing that no longer has it. One that finds it there
	// owes nothing any more.
	//
	// A read that failed, where a listing left the instance out while it was
	// out, leaves the listing's verdict as the newest news there is: the
	// instance goes (listedGone).
	switch {
	case !landed:
		if owed, ok := p.owedDelete[c.key]; ok && !owed.gone.IsZero() && p.reads.calls[c.key] == nil {
			p.accept(ctx, iutil.NewEvent(time.Now(), incusapi.EventLifecycleInstanceDeleted, c.project, c.name, ""))
		}
	case p.state.instance(c.project, c.name) != nil:
		delete(p.owedDelete, c.key)

		p.readAt[subject{project: c.project, instance: c.name}] = c.sent
	case had && p.reads.calls[c.key] != nil:
		owed := p.owedDelete[c.key]
		owed.subject = subject{project: c.project, instance: c.name}
		owed.since = c.sent
		p.owedDelete[c.key] = owed
	case had:
		p.announceDelete(c.project, c.name)
	}
}

// sessionOf applies the stream session ev was minted in, starting the run for
// a session newly up, and reports whether ev belongs to the newest session.
//
// The invariant every consumer of the chain keeps (see sessionOf in
// cmd/ic-dns/dns): session numbers only grow, and the newest seen is kept.
// An event of an older session is a leftover - a connected kept in its slot
// while a newer one went through the inbox, a disconnected late out of a
// queue - and changes nothing: not the session the run reads for, nor the
// chain state. The first event of a newer session, of whatever kind, opens
// it: a stream coming back makes everything held here suspect at once,
// whatever happened while it was down was announced to nobody, and the run
// starts again for that session - its connected may never have got here.
// Its disconnected closes it, and nothing of it reopens it. A connected of
// the session already open changes nothing.
//
// The newest seen is the newer of the session the run reads for and the one
// a disconnected last closed. A connected no source minted - session 0, as
// a test feeds them - restarts the run as it comes.
func (p *Plugin) sessionOf(ctx context.Context, ev *iutil.Event) bool {
	session := ev.Session()
	newest := max(p.sweepSession, p.closedSession)

	switch {
	case session == 0:
		if ev.Action() == iutil.ActionConnected {
			p.sweepSession = 0
			p.restartSweep(ctx)
		}

		return true

	case session < newest:
		return false

	case session > newest:
		if ev.Action() == iutil.ActionDisconnected {
			p.closedSession = session

			return true
		}

		p.sweepSession = session
		p.restartSweep(ctx)

		return true
	}

	if ev.Action() == iutil.ActionDisconnected {
		p.closedSession = session
	}

	return true
}

// newestSession is the newest session there is news of: the newer of what the
// fold has taken and what the door has seen. A plugin after this one may know
// a session the fold has not taken yet - its first event dropped on a full
// inbox and handed on, its connected still held - but never one the door has
// not seen.
func (p *Plugin) newestSession() uint64 {
	return max(p.sweepSession, p.closedSession, p.held.newestSeen())
}

// isCurrent reports whether ev is of the newest session there is news of, or
// of none: one whose data the plugins after this one fold.
//
// The invariant it keeps: what this plugin holds - the archive, and the
// state - takes in only what goes on in a session those plugins accept. An
// event they will fold nothing of must not change what is held here, or the
// next answer that does go on in an accepted session is held back as nothing
// new (the archive), or never made (the state, which a listing is compared
// against). Judged against the newest session seen at the door, since what
// was seen there may already have gone on.
func (p *Plugin) isCurrent(ev *iutil.Event) bool {
	return ev.Session() == 0 || ev.Session() >= p.newestSession()
}

// stamp is an event this plugin minted, marked the way the source marks what
// it mints: the chain, and the newest session there is news of, so a plugin
// after this one folds what this plugin found and judges it by the session
// rather than its time. What this plugin mints - a read's answer, a delete
// a read or a listing found - is news as of now, whatever session asked for
// it. While the door has seen a session the fold has not taken yet, the chain
// is cold: nothing of that session has been read whole.
func (p *Plugin) stamp(ev *iutil.Event) *iutil.Event {
	newest := p.newestSession()

	chain := p.chain
	if newest > max(p.sweepSession, p.closedSession) {
		chain = iutil.ChainCold
	}

	ev = ev.WithChainState(chain)

	if ev.Session() == 0 || ev.Session() < newest {
		ev = ev.WithSession(newest)
	}

	return ev
}

// owedDelete is one delete a key owes: the instance, and when the read that
// found it gone went out - zero where none did, and it is a listing that left
// it out. gone is when the newest listing that left it out, while a read of
// it was out, went out: zero while none has.
type owedDelete struct {
	subject

	since, gone time.Time
}

// listedGone takes a listing asked for at that leaves out an instance held,
// or one owed a delete. One asked for before the read that found it gone says
// nothing new. With no read of it out, the listing decides, and the instance
// goes. With one out, the delete does not go yet: the events that read holds
// are ahead of where a delete would go. The verdict waits for it, owed, and
// the end of the round with it (endIfRead), or the round would end with the
// instance still held downstream. A read that went out after the listing
// knows more, and decides: found, the instance stays; gone, it goes. One that
// went before it does not decide: whatever it answers - found, failed - is no
// newer than the listing, an instance found may have gone after, its delete
// lost with the stream. With no read asked for after the listing out once
// the read lands, the verdict is final (settleRead): a read that failed says
// nothing newer either.
func (p *Plugin) listedGone(ctx context.Context, key string, s subject, at time.Time) {
	owed, isOwed := p.owedDelete[key]
	if isOwed && !at.After(owed.since) {
		return
	}

	if p.reads.calls[key] == nil {
		p.accept(ctx, iutil.NewEvent(time.Now(), incusapi.EventLifecycleInstanceDeleted, s.project, s.instance, ""))

		return
	}

	owed.subject = s
	owed.gone = at
	p.owedDelete[key] = owed
}

// verdictsOwed reports whether a listing's verdict still waits for a read.
func (p *Plugin) verdictsOwed() bool {
	for _, owed := range p.owedDelete {
		if !owed.gone.IsZero() {
			return true
		}
	}

	return false
}

// announceDelete puts in the line the delete of one instance nothing announced
// and nothing holds any more, and settles what was owed for it.
func (p *Plugin) announceDelete(project, name string) {
	delete(p.owedDelete, resourceKey(kindInstance, project, name))

	p.q.push(p.stamp(iutil.NewEvent(time.Now(),
		incusapi.EventLifecycleInstanceDeleted, project, name, "")), true)
}

// accept puts one event in the line, issuing whatever read it needs first.
func (p *Plugin) accept(ctx context.Context, ev *iutil.Event) {
	current := p.sessionOf(ctx, ev)

	switch {
	case ev.ChainState() == "":
		// The run's own events arrive here unstamped: nothing minted them past
		// the source, so this plugin carries over what it last saw, and the
		// session it is reading for.
		ev = p.stamp(ev)
	case current:
		// What the chain is, as said in the newest session. A warm event of
		// the session before, late out of a queue, says nothing of a stream
		// that came back cold since.
		p.chain = ev.ChainState()
	}

	// A leftover of an older session changes nothing here either, data
	// included: a delete late out of a queue would take away an instance the
	// newer session has read. It goes on as it is, for the log and for what
	// counts drops; ic-dns folds nothing of it either. Older than the newest
	// session there is news of, not only than the one the fold has taken: a
	// plugin after this one may already be in that one (isCurrent).
	if ev.Action() == iutil.ActionConnected || !p.isCurrent(ev) {
		p.q.push(ev, true)

		return
	}

	want := p.args.Wanted[ev.Action()]

	// Before every push below, since what the project sets is the state's and
	// owes nothing to the read this event may or may not go on to need.
	ev = p.withProject(ev)

	p.readProject(ctx, ev)

	// Already finished, or no name to read: neither is worth a read, and both
	// keep their place rather than overtaking what is still waiting.
	if ev.Err() != nil || ev.Name() == "" {
		p.q.push(ev, true)

		return
	}

	// A network moving changes every record on it, so that path patches and fans
	// out rather than enriching the event before it.
	if p.acceptNetwork(ctx, ev) {
		return
	}

	// A profile re-expands every instance using it, so this fans out over the
	// state. Except a delete, which Incus refuses while anything still uses it.
	if strings.HasPrefix(ev.Action(), profilePrefix) {
		p.q.push(ev, true)

		if ev.Action() != incusapi.EventLifecycleProfileDeleted {
			p.fanOut(ctx, p.state.projectInstances(ev.ProjectName()))
		}

		return
	}

	// A delete is complete as it stands; reading to confirm it would answer a
	// question we already have the answer to.
	if ev.Action() == incusapi.EventLifecycleInstanceDeleted {
		p.state.deleteInstance(ev.ProjectName(), ev.Name())
		p.archive.forget(ev.ProjectName(), ev.Name())
		p.updateMetrics()
		p.q.push(ev, true)
		p.forgetUnread(ctx, resourceKey(kindInstance, ev.ProjectName(), ev.Name()))
		delete(p.owedDelete, resourceKey(kindInstance, ev.ProjectName(), ev.Name()))
		delete(p.readAt, subject{project: ev.ProjectName(), instance: ev.Name()})

		return
	}

	// A rename takes the old name out. The new one is read like any other
	// change, which is what the event that follows this line does.
	if ev.Action() == incusapi.EventLifecycleInstanceRenamed && ev.OldName() != "" {
		p.state.deleteInstance(ev.ProjectName(), ev.OldName())
		p.archive.forget(ev.ProjectName(), ev.OldName())
		p.updateMetrics()
		p.forgetUnread(ctx, resourceKey(kindInstance, ev.ProjectName(), ev.OldName()))
		delete(p.readAt, subject{project: ev.ProjectName(), instance: ev.OldName()})
	}

	// Both have to be true: wanted for instance enrichment, and the action
	// actually names an instance (a network action can be wanted too).
	instance := strings.HasPrefix(ev.Action(), instancePrefix) &&
		want.Enrich&wantsInstanceRead != 0

	if !instance {
		p.q.push(ev, true)

		return
	}

	it := p.q.push(ev, false)

	waitForRunning := ev.Action() == incusapi.EventLifecycleInstanceStarted ||
		ev.Action() == incusapi.EventLifecycleInstanceRestarted

	c := &call{
		key:            resourceKey(kindInstance, ev.ProjectName(), ev.Name()),
		kind:           kindInstance,
		project:        ev.ProjectName(),
		name:           ev.Name(),
		items:          []*item{it},
		wantInterfaces: want.Enrich&iutil.EnrichedInstanceWithInterfaces != 0,
		waitForRunning: waitForRunning,
	}

	p.reads.send(ctx, c)
}

// forgetUnread takes instances whose read failed off what the end of a run
// waits on, because they went: deleted, renamed away, or no longer listed.
func (p *Plugin) forgetUnread(ctx context.Context, keys ...string) {
	for _, key := range keys {
		delete(p.unread, key)
	}

	p.endIfRead(ctx)
}

// endIfRead announces the end of a run held at its end, once nothing it waits
// on is left: its project reads, and the instances whose read failed, and the
// listings' verdicts waiting for a read (listedGone), unless the grace for
// them is over. A verdict not waited for would end the round with the
// instance a listing left out still held downstream.
//
// The grace is armed here, the first time a run held at its end has anything
// unread - at its end, or when a read fails after it - so no wait on unread
// goes untimed.
func (p *Plugin) endIfRead(ctx context.Context) {
	if !p.sweepEnding {
		return
	}

	if (len(p.unread) > 0 || p.verdictsOwed()) && !p.graceOver {
		if p.endGrace == nil {
			p.endGrace = time.After(p.opts.roundGrace)

			p.logger.Info("the run waits on instances whose read failed",
				"plugin", name, "instances", len(p.unread), "grace", p.opts.roundGrace)
		}

		return
	}

	if p.reads.owes() > 0 {
		return
	}

	p.sweepEnd(ctx)
}

// fanOut re-reads a set of instances nothing named directly, each as an
// instance-updated of its own held on its call, going on only if the read
// found something new.
func (p *Plugin) fanOut(ctx context.Context, subjects iter.Seq[subject]) {
	wantInterfaces := p.args.Wanted[incusapi.EventLifecycleInstanceUpdated].Enrich&iutil.EnrichedInstanceWithInterfaces != 0

	for s := range subjects {
		p.reads.send(ctx, &call{
			key:     resourceKey(kindInstance, s.project, s.instance),
			kind:    kindInstance,
			project: s.project,
			name:    s.instance,
			ev: p.stamp(iutil.NewEvent(time.Now(),
				incusapi.EventLifecycleInstanceUpdated, s.project, s.instance, "")),
			wantInterfaces: wantInterfaces,
		})
	}
}

// patchState patches the state from one instance read and decides what that key is
// owed - the only place either happens.
//
// The second return says whether the read landed; false fails the events
// waiting on it, the same as a read that errored.
func (p *Plugin) patchState(
	ctx context.Context,
	project, name string,
	inst *incusapi.Instance,
	instanceState *incusapi.InstanceState,
	err error,
) (*iutil.Instance, bool) {
	switch {
	case err == nil:

	case incusapi.StatusErrorCheck(err, http.StatusNotFound):
		// Read after it went. settleRead announces the delete where this held
		// it, since one that went while nobody listened has no event coming.
		p.state.deleteInstance(project, name)
		p.retries.done(project, name)
		p.archive.forget(project, name)
		delete(p.readAt, subject{project: project, instance: name})

		return nil, true

	default:
		p.retries.soon(project, name)

		return nil, false
	}

	if p.opts.Instance != nil && inst != nil && !p.opts.Instance(inst) {
		p.state.deleteInstance(project, name)
		p.retries.done(project, name)
		p.archive.forget(project, name)
		delete(p.readAt, subject{project: project, instance: name})

		return nil, true
	}

	i, missing := p.state.setInstance(inst, instanceState)

	// A NIC on a network nothing has read yet: nothing was stored, so read the
	// network, and the re-read below places the NIC once it lands. The re-read
	// is what corrects it either way, since the network's own fan-out only
	// reaches instances the state already holds.
	if i == nil {
		p.readNetwork(ctx, project, missing)
		p.retries.soon(project, name)

		return nil, false
	}

	// No lease yet, or a state read that could not be made.
	if i.Running() && !addressed(i) {
		if isVMWithoutAgent(inst, instanceState) {
			p.retries.done(project, name)

			if p.opts.TTL > 0 {
				p.retries.at(project, name, p.opts.TTL)
			}

			return i, true
		}

		p.retries.soon(project, name)

		return i, true
	}

	p.retries.done(project, name)

	if p.opts.TTL > 0 {
		p.retries.at(project, name, p.opts.TTL)
	}

	return i, true
}
