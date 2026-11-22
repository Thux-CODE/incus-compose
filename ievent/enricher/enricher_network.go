package enricher

import (
	"context"
	"slices"
	"time"

	incusapi "github.com/lxc/incus/v7/shared/api"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// networkActions are the actions whose subject is a network itself. A peer,
// an ACL or a forward shares the "network-" prefix but names something on a
// network, and reading it as a network would ask Incus for one called that.
var networkActions = []string{
	incusapi.EventLifecycleNetworkCreated,
	incusapi.EventLifecycleNetworkUpdated,
	incusapi.EventLifecycleNetworkDeleted,
	incusapi.EventLifecycleNetworkRenamed,
}

// acceptNetwork handles one network action, and reports whether it was one.
// The event itself carries no enrichment, since its subject is a network; what
// it causes is the patch, and the re-read of everything on that network.
func (p *Plugin) acceptNetwork(ctx context.Context, ev *iutil.Event) bool {
	if !slices.Contains(networkActions, ev.Action()) {
		return false
	}

	// The event that happened goes in first, ahead of anything it causes.
	p.q.push(ev, true)

	// What was read of it before now is older than this, wherever it lands.
	p.networkMovedNow(ev.ProjectName(), ev.Name())

	if ev.OldName() != "" {
		p.networkMovedNow(ev.ProjectName(), ev.OldName())
	}

	// A rename leaves the old key behind; everything that was on it is re-read,
	// because the key its addresses were filed under has gone.
	if ev.OldName() != "" {
		p.forgetNetwork(ctx, ev.ProjectName(), ev.OldName())
	}

	if ev.Action() == incusapi.EventLifecycleNetworkDeleted {
		p.forgetNetwork(ctx, ev.ProjectName(), ev.Name())

		return true
	}

	// Created, updated or renamed: the network is read and patched first, so the
	// re-reads resolve against the new subnet rather than the old one.
	p.reads.send(ctx, &call{
		key:     resourceKey(kindNetwork, ev.ProjectName(), ev.Name()),
		kind:    kindNetwork,
		project: ev.ProjectName(),
		name:    ev.Name(),
	})

	return true
}

// networkMovedNow records an event about one network, now.
func (p *Plugin) networkMovedNow(project, name string) {
	if p.netMoved[project] == nil {
		p.netMoved[project] = map[string]time.Time{}
	}

	p.netMoved[project][name] = time.Now()
}

// networkMoved reports whether an event about one network arrived after since:
// a read or a listing of it from before then says what the event changed.
func (p *Plugin) networkMoved(project, name string, since time.Time) bool {
	at, moved := p.netMoved[project][name]

	return moved && at.After(since)
}

// settleNetworkMoves forgets what a listing of a project's networks asked for
// at has made old news: an event before it is one the listing knew of, and
// with no network read out - one of those may have been asked for before the
// event, and still has to be told older - nothing else can be. Otherwise the
// history grows by every network name the project ever had.
func (p *Plugin) settleNetworkMoves(project string, at time.Time) {
	for _, c := range p.reads.calls {
		if c.kind == kindNetwork {
			return
		}
	}

	for name, moved := range p.netMoved[project] {
		if moved.Before(at) {
			delete(p.netMoved[project], name)
		}
	}

	if len(p.netMoved[project]) == 0 {
		delete(p.netMoved, project)
	}
}

// forgetNetwork drops one network and re-reads what was on it. Both halves matter:
// dropping alone leaves addresses filed under a key nothing describes any
// more, and re-reading alone leaves the network there to resolve against.
//
// A read of it still out was taken before it went, and landing it would put
// the network back under the key just dropped: it is abandoned instead.
func (p *Plugin) forgetNetwork(ctx context.Context, project, name string) {
	on := p.state.networkInstances(project, name)

	p.reads.cancel(resourceKey(kindNetwork, project, name))
	p.state.deleteNetwork(project, name)
	p.fanOut(ctx, on)
}
