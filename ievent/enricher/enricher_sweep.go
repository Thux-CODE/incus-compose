package enricher

import (
	"context"
	"slices"
	"time"

	incusapi "github.com/lxc/incus/v7/shared/api"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// sweepArgs is what every run this plugin starts is given. The reads are the
// plugin's own, so a test that answers them answers the run as well.
func (p *Plugin) sweepArgs() sweepArgs {
	return sweepArgs{
		conn:     p.reads.fleet,
		readNet:  p.reads.readNet,
		sweepOut: p.sweeps,
		evOut:    p.inbox,
		serves:   p.opts.Project,
		timeout:  p.opts.ReadTimeout,
		delay:    p.opts.ReadDelay,
		retry:    p.opts.sweepRetry,
		retryMax: p.opts.sweepRetryMax,
		run:      p.sweepRun,
	}
}

// restartSweep abandons the run where it stands and starts a fast one. What a
// reconnect asks for: everything held is as old as the stream was down.
//
// The abandoned run's end is abandoned with it, held or not: it was read
// before the stream came back, and announcing it would turn the chain warm on
// a fleet nobody has read since.
func (p *Plugin) restartSweep(ctx context.Context) {
	if p.sweeperCancel != nil {
		p.sweeperCancel()
	}

	p.sweepRun++
	p.sweepEnding = false
	p.endGrace = nil
	p.graceOver = false

	sweepCtx, stopSweeping := context.WithCancel(ctx)
	p.sweeperCancel = stopSweeping

	runSweeper(sweepCtx, p.logger, p.sweepArgs(), p.opts.SweepInterval)
}

// acceptSweep takes one message from the sweeper, unless it is from a run
// already abandoned.
func (p *Plugin) acceptSweep(ctx context.Context, msg sweepMsg) {
	if msg.run != p.sweepRun {
		return
	}

	switch msg.action {
	case sweepActionProjects:
		// Announced before the prune, the way a listing of instances is: a
		// project that went, or one no longer served, would otherwise leave its
		// names answering - restored ones most of all, which nothing else reads.
		for _, s := range p.state.missingProjects(msg.names) {
			p.accept(ctx, iutil.NewEvent(time.Now(),
				incusapi.EventLifecycleInstanceDeleted, s.project, s.instance, ""))
		}

		for _, s := range p.owedDelete {
			if !slices.Contains(msg.names, s.project) {
				p.accept(ctx, iutil.NewEvent(time.Now(),
					incusapi.EventLifecycleInstanceDeleted, s.project, s.instance, ""))
			}
		}

		p.state.pruneProjects(msg.names)

		p.forgetUnread(ctx, p.unreadOutside(func(s subject) bool { return slices.Contains(msg.names, s.project) })...)

		// The run lists, this plugin reads: a project's own configuration is
		// what an instance event in it carries, and it goes through the pool
		// like every other read this plugin makes.
		for _, project := range msg.names {
			p.reads.send(ctx, &call{
				key:     resourceKey(kindProject, project, ""),
				kind:    kindProject,
				project: project,
			})
		}

		for ev := range p.q.release() {
			p.args.Next(ev)
		}

	case sweepActionNetworks:
		p.state.pruneNetworks(msg.project, msg.names)

	case sweepActionInstances:
		// Through accept, which is where a delete drops the instance and its
		// archive entry: a name Incus no longer lists is gone the same way one
		// it announced is, and nothing downstream can tell them apart.
		for _, s := range p.state.missingInstances(msg.project, msg.names) {
			p.accept(ctx, iutil.NewEvent(time.Now(),
				incusapi.EventLifecycleInstanceDeleted, s.project, s.instance, ""))
		}

		// And what is owed a delete nothing holds any more.
		for _, s := range p.owedDelete {
			if s.project == msg.project && !slices.Contains(msg.names, s.instance) {
				p.accept(ctx, iutil.NewEvent(time.Now(),
					incusapi.EventLifecycleInstanceDeleted, s.project, s.instance, ""))
			}
		}

		// What the listing has and nothing holds is a name nobody downstream
		// has yet: the end of the run waits until its read lands, so a read
		// that fails cannot leave the run whole without it. One whose read
		// failed and that the listing no longer has went before anything held
		// it, so there is no delete to make, only no wait.
		for _, instance := range msg.names {
			if p.state.instance(msg.project, instance) == nil {
				p.unread[resourceKey(kindInstance, msg.project, instance)] = subject{project: msg.project, instance: instance}
			}
		}

		p.forgetUnread(ctx, p.unreadOutside(func(s subject) bool {
			return s.project != msg.project || slices.Contains(msg.names, s.instance)
		})...)

		for ev := range p.q.release() {
			p.args.Next(ev)
		}

	case sweepActionNetwork:
		p.state.setNetwork(*msg.network)

	case sweepActionNetworksWarm:
		// Every network an instance could sit on is now known, so the reads held
		// back until one could be placed may go.
		p.reads.flush(ctx)

	case sweepActionDone:
		// Held back while the run's own project reads are still out: warm says
		// the fleet has been read whole, and it has not been until they land.
		// A run over a project with no instances reaches here first. So is one
		// with an instance whose read failed and that nothing holds, for as
		// long as the grace: its retry is what ends the run.
		p.sweepEnding = true

		p.endIfRead(ctx)

	case sweepActionFailed:
		if p.opts.Metrics {
			sweepsTotal.WithLabelValues("failed").Inc()
		}
		p.logger.Warn("the run could not read part of the fleet, serving what is held",
			"plugin", name, "err", msg.err)
	}
}

// unreadOutside is the keys of the instances whose read failed that keep does
// not keep.
func (p *Plugin) unreadOutside(keep func(subject) bool) []string {
	var out []string

	for key, s := range p.unread {
		if !keep(s) {
			out = append(out, key)
		}
	}

	return out
}

// sweepEnd says a run has been all the way round.
//
// At the head rather than through next, so debounce sees the end of a run too.
// Warm rides with it: this plugin is what decides the fleet has been read whole,
// and a run reaching its end is when that becomes true.
func (p *Plugin) sweepEnd(ctx context.Context) {
	p.sweepEnding = false
	p.endGrace = nil
	p.graceOver = false

	if p.opts.Metrics {
		sweepsTotal.WithLabelValues("success").Inc()
	}
	p.updateMetrics()

	select {
	case p.args.CommandOut <- iutil.Command{Action: iutil.ActionSweepEnd, ChainState: iutil.ChainWarm, Session: p.sweepSession}:
	case <-ctx.Done():
	}
}
