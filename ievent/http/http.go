// Package http serves /metrics, /health and /ready.
package http

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// name is what this plugin is called, in the chain and in a drop's reason.
const name = "http"

// Timeouts for the server. Small, because everything it answers is a field read.
const (
	readTimeout     = 5 * time.Second
	writeTimeout    = 10 * time.Second
	shutdownTimeout = 5 * time.Second
)

// Config is where and what to serve.
type Config struct {
	// Listen is the address to answer on; empty serves nothing.
	Listen string

	// Silence is how long the chain may say nothing before /health fails; zero never fails on silence.
	Silence time.Duration

	// Metrics turns the /metrics endpoint on.
	Metrics bool

	// Pprof enables /debug/pprof; never on in a deployment.
	Pprof bool

	// Gate is asked once the round has latched: a non-nil error keeps /ready
	// at 503 with it as the reason. For a plugin that needs more than a round
	// to answer, read off Incus apart from the events. Nil asks nothing.
	Gate func() error
}

// Plugin answers the observability endpoints.
type Plugin struct {
	cfg Config

	next iutil.Next

	// in is the source asking this plugin to finish; out is the answer.
	in  <-chan iutil.Command
	out chan<- iutil.Command

	// ready latches on the first warm event since the stream was last lost, and
	// clears only when it is lost again: see Handle.
	ready atomic.Bool

	// lost is when the stream last went, so a warm event stamped before it - a
	// debounced leftover - is not taken for one since.
	lost time.Time

	// mu orders the decisions on ready against each other. Handle has more
	// than one caller at once - a plugin before this one hands on what it
	// drops from its own caller's goroutine while its fold hands on the rest -
	// and a warm event that read lost before a disconnect stored it would
	// otherwise set ready again after the disconnect cleared it.
	mu sync.Mutex

	// connected is the source's own state, straight off the chain.
	connected atomic.Bool

	// lastEvent is when something last walked past, as UnixNano.
	lastEvent atomic.Int64
}

var _ iutil.Plugin = (*Plugin)(nil)

// Option sets one field of Config; the zero value means unset.
type Option func(*Config)

// Listen sets the address to answer on. Empty serves nothing.
func Listen(addr string) Option { return func(cfg *Config) { cfg.Listen = addr } }

// Silence sets how long the chain may say nothing before /health fails; zero never fails.
func Silence(d time.Duration) Option { return func(cfg *Config) { cfg.Silence = d } }

// Metrics turns the /metrics endpoint on.
func Metrics(v bool) Option { return func(cfg *Config) { cfg.Metrics = v } }

// Pprof turns /debug/pprof on; not for a deployment.
func Pprof(v bool) Option { return func(cfg *Config) { cfg.Pprof = v } }

// Gate adds a condition /ready waits on after the round, nil once met.
func Gate(gate func() error) Option { return func(cfg *Config) { cfg.Gate = gate } }

// New builds the endpoint server. It starts nothing: Run owns the goroutine.
func New(logger *slog.Logger, opts ...Option) *Plugin {
	var cfg Config

	for _, opt := range opts {
		opt(&cfg)
	}

	logger.Info("Starting", "plugin", name, "config", cfg)

	p := &Plugin{cfg: cfg}
	p.lastEvent.Store(time.Now().UnixNano())

	return p
}

// Name identifies the plugin in logs, in metrics, and in the chain.
func (p *Plugin) Name() string { return name }

// Wants nothing: it is in the chain, so it sees whatever walks.
func (p *Plugin) Wants() []iutil.Want { return nil }

// Setup keeps the successor and starts nothing.
func (p *Plugin) Setup(args iutil.SetupArgs) error {
	p.next = args.Next
	p.in, p.out = args.CommandIn, args.CommandOut

	return nil
}

// Handle folds the event into what is served and hands it straight on, on the caller's goroutine.
func (p *Plugin) Handle(ev *iutil.Event) {
	p.lastEvent.Store(time.Now().UnixNano())

	p.decide(ev)

	p.next(ev)
}

// decide moves readiness on one event, under mu.
func (p *Plugin) decide(ev *iutil.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch ev.Action() {
	case iutil.ActionConnected:
		p.connected.Store(true)

		return

	case iutil.ActionDisconnected:
		// The one thing that takes readiness back: nothing confirms what is
		// served until a round has been all the way round again.
		p.connected.Store(false)
		p.ready.Store(false)
		p.lost = ev.At()

		return
	}

	// Cold says when an event was minted, not what is served now: a read that
	// failed, or one sent before the round ended and landing after it, still
	// carries the stamp it was made with, and a round that has ended stays
	// ended. Only a warm event turns readiness on - and only one nobody
	// dropped, since a dropped one was folded by nobody before here and may
	// have overtaken what it follows.
	if ev.ChainState() == iutil.ChainWarm && ev.Err() == nil && ev.At().After(p.lost) {
		p.ready.Store(true)
	}
}

// Run serves until ctx is canceled; it blocks, so main owns the goroutine.
func (p *Plugin) Run(ctx context.Context) error {
	if p.cfg.Listen == "" {
		p.wait(ctx)

		return nil
	}

	mux := http.NewServeMux()

	if p.cfg.Metrics {
		mux.Handle("GET /metrics", promhttp.Handler())
	}

	mux.HandleFunc("GET /health", p.health)
	mux.HandleFunc("GET /ready", p.serveReady)

	// No method prefix: `go tool pprof` resolves symbols with a POST, and the
	// trailing slash is what routes /heap, /goroutine and /allocs to Index.
	if p.cfg.Pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	// ?seconds=30 only answers once finished, so the write timeout would cut it short.
	write := writeTimeout
	if p.cfg.Pprof {
		write = 0
	}

	server := &http.Server{
		Addr:         p.cfg.Listen,
		Handler:      mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: write,
	}

	errs := make(chan error, 1)

	go func() {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	case cmd := <-p.in:
		// Nothing is held here, so there is nothing to drain.
		select {
		case p.out <- cmd:
		case <-ctx.Done():
		}
	}

	// Its own budget: ctx may already be done, and inheriting it would cut live connections.
	shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	return server.Shutdown(shutCtx)
}

// wait holds until told to finish, for a build that asked for no endpoints.
func (p *Plugin) wait(ctx context.Context) {
	select {
	case <-ctx.Done():
	case cmd := <-p.in:
		select {
		case p.out <- cmd:
		case <-ctx.Done():
		}
	}
}

// health answers whether the process is worth keeping, not whether it is ready.
func (p *Plugin) health(w http.ResponseWriter, _ *http.Request) {
	if p.cfg.Silence > 0 {
		since := time.Since(time.Unix(0, p.lastEvent.Load()))
		if since > p.cfg.Silence {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("no event for " + since.Round(time.Second).String() + "\n"))

			return
		}
	}

	_, _ = w.Write([]byte("ok\n"))
}

// serveReady answers whether there is anything worth sending traffic at.
func (p *Plugin) serveReady(w http.ResponseWriter, _ *http.Request) {
	switch {
	case !p.connected.Load():
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("the Incus event stream is not connected\n"))

	case !p.ready.Load():
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("nothing published yet\n"))

	default:
		// Asked once: the answer may move between two calls.
		if p.cfg.Gate != nil {
			err := p.cfg.Gate()
			if err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(err.Error() + "\n"))

				return
			}
		}

		_, _ = w.Write([]byte("ready\n"))
	}
}

// _ pins the interface here, so a change to it fails the build at the plugin.
var _ iutil.Plugin = (*Plugin)(nil)
