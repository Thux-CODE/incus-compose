package ecs_view

import (
	"context"
	"log/slog"
	"net/netip"
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
	// answers are clamped so stale records expire fast.
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

	// Trusted are the queriers exempt from the filter, set once before anything
	// serves. A query arriving from one of these prefixes is answered from every
	// view at once rather than from the one its address sits on: what a router
	// reaching every network needs, and nothing else should be given. Empty
	// trusts nobody, which is the filter as it stands without this.
	Trusted []netip.Prefix

	// current is the published snapshot.
	current atomic.Pointer[Snapshot]

	// healthy is false while the source says its data is stale, which clamps the
	// TTL.
	healthy atomic.Bool

	// published says a source has handed over at least one snapshot, which an
	// empty fleet does and a cold start has not.
	published atomic.Bool
}

// New returns an ECSView already holding an empty snapshot, so the query path
// never has to check for nil.
func New(logger *slog.Logger) *ECSView {
	v := &ECSView{logger: logger}
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
}

// trusts says whether a query from addr is exempt from the filter. The address
// is the one the packet arrived from: an ECS option is what a querier claims
// about itself, and trust may not rest on a claim.
func (v *ECSView) trusts(addr netip.Addr) bool {
	for _, prefix := range v.Trusted {
		if prefix.Contains(addr) {
			return true
		}
	}

	return false
}

// Ready turns true once a snapshot has been published and the source reports
// itself healthy.
func (v *ECSView) Ready() bool {
	return v.published.Load() && v.healthy.Load()
}
