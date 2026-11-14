package ecs_view

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/coredns/coredns/plugin"
	"github.com/miekg/dns"
)

// Source is a plugin that feeds views to ecs_view.
type Source interface {
	// SetSink is called once at startup, before the source produces anything.
	SetSink(Sink)
}

// Sink is where a source publishes.
type Sink interface {
	// Replace swaps everything served for snap, deleting by absence. The snapshot
	// must be finished and unreferenced: from here on every reader shares it.
	Replace(snap *Snapshot)
	// SetHealthy says whether the source's data is fresh. While it is not,
	// answers are clamped so stale records expire fast. The first true is also
	// what turns the view warm, for good: see ECSView.Claims.
	SetHealthy(healthy bool)
}

// ECSView serves records filtered per querier: a read-only view onto what a
// source publishes, deriving and accumulating nothing.
type ECSView struct {
	logger *slog.Logger

	Next plugin.Handler

	// Server labels this engine's metrics. A field rather than the context key
	// CoreDNS uses, which core/dnsserver fills and nothing here runs.
	Server string

	// EchoSubnet turns on the RFC 7871 reply option, set once before anything
	// serves. Off by default: it costs an allocation and only an ECS-aware cache reads it.
	EchoSubnet bool

	// Metrics turns on the counters and gauges.
	Metrics bool

	// Claims holds the domains this server answers for whether or not a zone in
	// them has been published yet, set once before anything serves. Until warm,
	// a miss under one of them is SERVFAIL rather than a fall through: the name
	// may only not have been read yet, and a forwarder's NXDOMAIN would be a
	// final answer that also stops the client trying its next server. A miss in
	// no zone we know is still asked of Next, and a NOERROR, NODATA included, or
	// a truncated reply is passed on - a host sharing the suffix resolves by
	// forwarding - while anything else from it fails the same way.
	Claims []string

	// ColdAnswers serves what the zones hold before warm, clamped like any
	// unhealthy answer. Off, every query inside a zone is SERVFAIL until warm,
	// the same as a miss, so a client with another server listed asks that one
	// instead of taking a record that may be out of date. Set once before
	// anything serves; New turns it on.
	ColdAnswers bool

	// current is the published snapshot.
	current atomic.Pointer[Snapshot]

	// healthy is false while the source says its data is stale, which clamps the
	// TTL.
	healthy atomic.Bool

	// published says a source has handed over at least one snapshot, which an
	// empty fleet does and a cold start has not.
	published atomic.Bool

	// warm says the source has once reported its data fresh: for incus, a round
	// having reached its end, holes and all. Never cleared: once the zones are full, a lost stream
	// leaves them as good as any other server's, and failing every miss for it
	// would fail them on every replica at once.
	warm atomic.Bool
}

// New returns an ECSView already holding an empty snapshot, so the query path
// never has to check for nil.
func New(logger *slog.Logger) *ECSView {
	v := &ECSView{logger: logger, ColdAnswers: true}
	v.current.Store(EmptySnapshot())

	return v
}

// Name implements the plugin.Handler interface.
func (v *ECSView) Name() string { return Name }

// ServeDNS implements plugin.Handler. A name outside every live zone falls
// through to the next plugin.
func (v *ECSView) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	handled, code, err := v.Answer(ctx, w, r)
	if handled {
		return code, err
	}

	return plugin.NextOrFailure(v.Name(), v.Next, ctx, w, r)
}

// Replace implements Sink: a pointer swap, unchanged.
func (v *ECSView) Replace(snap *Snapshot) {
	v.current.Store(snap)
	v.published.Store(true)

	if v.Metrics {
		zonesGauge.Set(float64(snap.Denial.Len()))
		addressesGauge.Set(float64(snap.ByIPv4.Len() + snap.ByIPv6.Len()))
	}
}

// SetHealthy implements Sink.
func (v *ECSView) SetHealthy(healthy bool) {
	v.healthy.Store(healthy)

	if healthy {
		v.warm.Store(true)
	}
}

// Ready turns true once a snapshot has been published and the source reports
// itself healthy, and false again only when the source reports itself
// unhealthy: warm stays where it is.
func (v *ECSView) Ready() bool {
	return v.published.Load() && v.healthy.Load()
}
