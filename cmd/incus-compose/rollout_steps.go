package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/lxc/incus-compose/shared"
)

var (
	errRolloutUnhealthy = errors.New("reported unhealthy")
	errRolloutTimeout   = errors.New("not healthy in time")
	errRolloutStopped   = errors.New("stopped before it was healthy")
)

// rolloutHealthReads is how many reads of a starting replica may fail in a row.
const rolloutHealthReads = 3

// rolloutOps is what a rollout does to the replicas of one service, by index.
type rolloutOps interface {
	// create creates the replica from the compose file and starts it, without
	// waiting for its healthcheck. The first one a rollout creates runs the
	// service's pre_start hooks that are not per replica.
	create(ctx context.Context, index int, first bool) error

	// health returns the replica's user.healthcheck.status and whether it runs.
	health(ctx context.Context, index int) (string, bool, error)

	// drain sets or clears the replica's drain label.
	drain(ctx context.Context, index int, drained bool) error

	// retire stops the replica within its grace period and deletes it.
	retire(ctx context.Context, index int) error

	// discard deletes a new replica that did not become healthy, or with keep
	// leaves it stopped.
	discard(ctx context.Context, index int, keep bool) error

	// sleep waits for d, or until ctx is done.
	sleep(ctx context.Context, d time.Duration) error
}

// replica is one index of a service as the rollout sees it.
type replica struct {
	index int

	// current is made from the service config and the image as they are now:
	// what a create of the rollout would make (rolloutCurrent).
	current bool

	// fresh is a replica the rollout does not replace: one it created, or one
	// an earlier run left up to date (rolloutResumed).
	fresh bool

	running bool
	drained bool
}

// rolloutPlan is one service's rollout: what exists, and how long to wait.
type rolloutPlan struct {
	Service  string
	Replicas int
	Present  []replica

	// Name returns the instance name of an index.
	Name func(index int) string

	// KeepFailed leaves a new replica that failed stopped, for a look at it.
	KeepFailed bool

	HealthTimeout time.Duration
	Grace         time.Duration
	Poll          time.Duration
	Now           func() time.Time
	Out           io.Writer
}

// rolloutDescribe prints what a rollout of the plan would do, for --dry-run.
// A replica an earlier run left up to date reads "up to date" and takes no
// create, so a rollout picked up again may take fewer creates than replicas,
// none included.
func rolloutDescribe(plan rolloutPlan) {
	resumed := rolloutResumed(plan.Present)
	present := make([]string, 0, len(plan.Present))

	for _, r := range plan.Present {
		state := "stopped"
		if r.running {
			state = "running"
		}

		if r.drained {
			state += ", drained"
		}

		if resumed[r.index] {
			state += ", up to date"
		}

		present = append(present, fmt.Sprintf("%s (%s)", plan.Name(r.index), state))
	}

	noun := "replicas"
	if plan.Replicas == 1 {
		noun = "replica"
	}

	creates := max(plan.Replicas-len(resumed), 0)

	createNoun := "creates"
	if creates == 1 {
		createNoun = "create"
	}

	_, _ = fmt.Fprintf(plan.Out, "%s: can be rolled out, %d %s, now %s; %d %s, each new replica healthy within %s, each old one kept %s after its drain\n",
		plan.Service, plan.Replicas, noun, strings.Join(present, ", "), creates, createNoun, plan.HealthTimeout, plan.Grace)
}

// rolloutResumed returns the replicas an earlier, interrupted rollout left up
// to date, which this one keeps: current, running and not drained, next to at
// least one that is not current. When all are current nothing tells the ones
// an earlier run made from the ones it was to replace, and a rollout run on an
// unchanged service is asked to replace them all, so none is kept.
func rolloutResumed(present []replica) map[int]bool {
	out := map[int]bool{}

	if !slices.ContainsFunc(present, func(r replica) bool { return !r.current }) {
		return out
	}

	for _, r := range present {
		if r.current && r.running && !r.drained {
			out[r.index] = true
		}
	}

	return out
}

// rollout replaces every replica of a service with one created from the
// compose file, one at a time and new before old: a replica is retired only
// once the one that takes its place reports healthy.
//
// A service is its replicas, whatever their indexes, which is also how `up`
// reconciles it. So a new replica takes the lowest free index, and the old one
// it replaces leaves its index free for the next: N creates for N replicas,
// and with one replica `web-1` becomes `web-2`, and `web-1` again on the next
// rollout.
//
// A new replica that fails, or a rollout interrupted before it is healthy,
// is taken away again and leaves the old ones as they were. A rollout run
// again picks up from whatever an earlier one left: the replicas that one made
// are kept once healthy (rolloutResumed), and one it left drained is removed
// first when the others serve without it (rolloutSettleDrained).
func rollout(ctx context.Context, ops rolloutOps, plan rolloutPlan) error {
	slots := map[int]*replica{}
	for _, r := range plan.Present {
		slots[r.index] = &r
	}

	err := rolloutSteps(ctx, ops, plan, slots)
	if err != nil {
		_, _ = fmt.Fprintf(plan.Out, "%s: stopped, left %s\n", plan.Service, rolloutState(plan, slots))

		if len(rolloutOld(slots)) > 0 {
			_, _ = fmt.Fprintf(plan.Out, "%s: run `rollout %s` again to replace the old replicas\n", plan.Service, plan.Service)
		}

		return err
	}

	_, _ = fmt.Fprintf(plan.Out, "%s: rolled out, %s\n", plan.Service, rolloutState(plan, slots))

	return nil
}

func rolloutSteps(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica) error {
	n := plan.Replicas

	// What an earlier run made is kept once it is healthy. One that is not
	// is old again: it is not taken away here, as a new one would be, since
	// the service may have nothing else running; the surplus below gives
	// back the unhealthy one first, and the run stops before a create.
	var failed error

	for _, i := range slices.Sorted(maps.Keys(rolloutResumed(plan.Present))) {
		r := slots[i]
		r.fresh = true

		_, _ = fmt.Fprintf(plan.Out, "%s: up to date, left by an earlier rollout; waiting up to %s for healthy\n", plan.Name(i), plan.HealthTimeout)

		err := rolloutWaitHealthy(ctx, ops, plan, i)
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("interrupted: %w", ctx.Err())
			}

			r.fresh = false
			failed = errors.Join(failed, fmt.Errorf("%s: %w", plan.Name(i), err))
		}
	}

	err := rolloutSettleDrained(ctx, ops, plan, slots)
	if err != nil {
		return errors.Join(failed, err)
	}

	first := true

	for {
		old := rolloutOld(slots)
		if len(old) == 0 && len(slots) >= n {
			return failed
		}

		err := ctx.Err()
		if err != nil {
			return errors.Join(failed, fmt.Errorf("interrupted: %w", err))
		}

		if len(slots) > n {
			// One replica more than declared, left by an earlier run cut short:
			// one goes before another is created, so there are never two more.
			err := rolloutGiveBack(ctx, ops, plan, slots)
			if err != nil {
				return errors.Join(failed, err)
			}

			continue
		}

		if failed != nil {
			return failed
		}

		index := rolloutFree(slots)

		_, _ = fmt.Fprintf(plan.Out, "%s: creating\n", plan.Name(index))

		slots[index] = &replica{index: index, current: true, fresh: true, running: true}

		err = ops.create(ctx, index, first)
		if err == nil {
			_, _ = fmt.Fprintf(plan.Out, "%s: started, waiting up to %s for healthy\n", plan.Name(index), plan.HealthTimeout)

			err = rolloutWaitHealthy(ctx, ops, plan, index)
		}

		first = false

		if err != nil {
			if ctx.Err() != nil {
				err = fmt.Errorf("interrupted before it was healthy: %w", ctx.Err())
			}

			err = fmt.Errorf("%s: %w", plan.Name(index), err)

			discardErr := ops.discard(context.WithoutCancel(ctx), index, plan.KeepFailed)
			if discardErr != nil {
				return errors.Join(err, fmt.Errorf("taking %s away: %w", plan.Name(index), discardErr))
			}

			slots[index].running = false
			if !plan.KeepFailed {
				delete(slots, index)
			}

			return err
		}

		// Fewer replicas than declared were there: this one adds to them.
		if len(slots) <= n {
			continue
		}

		victim := rolloutVictim(slots, nil)

		err = rolloutRetire(ctx, ops, plan, slots[victim])
		if err != nil {
			return err
		}

		delete(slots, victim)
	}
}

// rolloutSettleDrained decides what becomes of the replicas found running
// drained. Only a rollout drains, and only right before a stop, so such a
// replica was left so by one cut short between its drain and its removal,
// once the one taking its place was healthy. With a replica more than
// declared, and the declared count serving without it (healthy, not drained),
// it stays drained and is the first to go; else it is put back and serves
// again until its turn comes.
func rolloutSettleDrained(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica) error {
	var drained []*replica

	for _, i := range slices.Sorted(maps.Keys(slots)) {
		r := slots[i]
		if r.running && r.drained {
			drained = append(drained, r)
		}
	}

	if len(drained) == 0 {
		return nil
	}

	surplus := len(slots) - plan.Replicas
	serving := 0

	for _, i := range slices.Sorted(maps.Keys(slots)) {
		r := slots[i]

		switch {
		case !r.running || r.drained:
		case r.fresh:
			// Waited for healthy already.
			serving++
		case surplus > 0:
			status, running, err := ops.health(ctx, i)
			if err != nil {
				return fmt.Errorf("%s: %w", plan.Name(i), err)
			}

			r.running = running
			if running && status == shared.HealthStatusHealthy {
				serving++
			}
		}
	}

	for _, r := range drained {
		if surplus > 0 && serving >= plan.Replicas {
			surplus--

			_, _ = fmt.Fprintf(plan.Out, "%s: drained by an earlier rollout, left so to be removed\n", plan.Name(r.index))

			continue
		}

		err := rolloutUndrain(ctx, ops, plan, r)
		if err != nil {
			return err
		}

		_, _ = fmt.Fprintf(plan.Out, "%s: drained by an earlier rollout, put back\n", plan.Name(r.index))
	}

	return nil
}

// rolloutOld returns the replicas this run replaces: neither created by it nor
// kept up to date from an earlier one.
func rolloutOld(slots map[int]*replica) []*replica {
	var old []*replica
	for _, r := range slots {
		if !r.fresh {
			old = append(old, r)
		}
	}

	return old
}

// rolloutFree returns the lowest index no replica holds.
func rolloutFree(slots map[int]*replica) int {
	for i := 1; ; i++ {
		_, taken := slots[i]
		if !taken {
			return i
		}
	}
}

// rolloutVictim picks the old replica to retire, of which there is at least
// one: one that does not run, is drained already or, where healthy says, is
// not healthy goes first, then the lowest index, which keeps the indexes a
// rollout cycles through low. It is the order in which `up` gives up a
// surplus (project.reconcileReplicas), with the replicas made from an older
// config, which `up` drops before the others, being here the only candidates,
// and the lowest index going where `up` drops the highest.
func rolloutVictim(slots map[int]*replica, healthy map[int]bool) int {
	rank := func(r *replica) int {
		switch {
		case !r.running:
			return 0
		case r.drained:
			return 1
		case healthy != nil && !healthy[r.index]:
			return 2
		default:
			return 3
		}
	}

	victim := slices.MinFunc(rolloutOld(slots), func(a, b *replica) int {
		d := rank(a) - rank(b)
		if d != 0 {
			return d
		}

		return a.index - b.index
	})

	return victim.index
}

// rolloutGiveBack retires one old replica of more than declared before
// anything new exists. A healthy one goes only when all the others are healthy
// too, so the service keeps its replicas serving.
func rolloutGiveBack(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica) error {
	healthy := map[int]bool{}

	for _, r := range slots {
		if !r.running || r.drained {
			continue
		}

		status, running, err := ops.health(ctx, r.index)
		if err != nil {
			return fmt.Errorf("%s: %w", plan.Name(r.index), err)
		}

		r.running = running
		healthy[r.index] = running && status == shared.HealthStatusHealthy
	}

	victim := rolloutVictim(slots, healthy)

	err := rolloutRetire(ctx, ops, plan, slots[victim])
	if err != nil {
		return err
	}

	delete(slots, victim)

	return nil
}

// rolloutRetire drains a running replica, gives clients the grace to move off
// it and retires it. An interrupt during the grace puts it back instead, so
// nothing is left drained; once the stop has begun it is seen through.
func rolloutRetire(ctx context.Context, ops rolloutOps, plan rolloutPlan, r *replica) error {
	name := plan.Name(r.index)

	if r.running {
		err := ctx.Err()
		if err != nil {
			return fmt.Errorf("interrupted: %w", err)
		}

		err = ops.drain(ctx, r.index, true)
		if err != nil {
			return errors.Join(fmt.Errorf("%s: draining: %w", name, err), rolloutUndrain(ctx, ops, plan, r))
		}

		r.drained = true

		_, _ = fmt.Fprintf(plan.Out, "%s: drained, waiting %s\n", name, plan.Grace)

		err = ops.sleep(ctx, plan.Grace)
		if err != nil {
			return errors.Join(fmt.Errorf("interrupted: %w", err), rolloutUndrain(ctx, ops, plan, r))
		}
	}

	err := ops.retire(context.WithoutCancel(ctx), r.index)
	if err != nil {
		return fmt.Errorf("%s: retiring: %w", name, err)
	}

	_, _ = fmt.Fprintf(plan.Out, "%s: removed\n", name)

	return nil
}

func rolloutUndrain(ctx context.Context, ops rolloutOps, plan rolloutPlan, r *replica) error {
	err := ops.drain(context.WithoutCancel(ctx), r.index, false)
	if err != nil {
		return fmt.Errorf("%s: still drained: %w", plan.Name(r.index), err)
	}

	r.drained = false

	return nil
}

// rolloutWaitHealthy waits for ic-healthd to call the replica healthy. Any
// other verdict ends the wait: unhealthy, a stop, or no verdict in time.
func rolloutWaitHealthy(ctx context.Context, ops rolloutOps, plan rolloutPlan, index int) error {
	start := plan.Now()
	last := "-"
	failed := 0

	for {
		status, running, err := ops.health(ctx, index)
		if err != nil {
			// A wait of minutes outlives a hiccup of the API.
			failed++
			if ctx.Err() != nil || failed >= rolloutHealthReads {
				return err
			}

			err = ops.sleep(ctx, plan.Poll)
			if err != nil {
				return fmt.Errorf("interrupted: %w", err)
			}

			continue
		}

		failed = 0

		elapsed := plan.Now().Sub(start).Round(time.Second)

		if !running {
			return fmt.Errorf("%w after %s", errRolloutStopped, elapsed)
		}

		if status != last {
			shown := status
			if shown == "" {
				shown = "not checked yet"
			}

			_, _ = fmt.Fprintf(plan.Out, "%s: %s (%s)\n", plan.Name(index), shown, elapsed)
			last = status
		}

		switch status {
		case shared.HealthStatusHealthy:
			return nil
		case shared.HealthStatusUnhealthy:
			return fmt.Errorf("%w after %s", errRolloutUnhealthy, elapsed)
		}

		if elapsed >= plan.HealthTimeout {
			return fmt.Errorf("%w (%s)", errRolloutTimeout, plan.HealthTimeout)
		}

		err = ops.sleep(ctx, plan.Poll)
		if err != nil {
			return fmt.Errorf("interrupted: %w", err)
		}
	}
}

// rolloutState describes what a rollout leaves, for the line it ends on.
func rolloutState(plan rolloutPlan, slots map[int]*replica) string {
	indexes := make([]int, 0, len(slots))
	for i := range slots {
		indexes = append(indexes, i)
	}
	slices.Sort(indexes)

	parts := make([]string, 0, len(indexes))
	for _, i := range indexes {
		r := slots[i]

		what := "old"
		if r.fresh {
			what = "new"
		}

		if !r.running {
			what += ", stopped"
		}

		if r.drained {
			what += ", drained"
		}

		parts = append(parts, fmt.Sprintf("%s (%s)", plan.Name(i), what))
	}

	if len(parts) == 0 {
		return "no replica"
	}

	return strings.Join(parts, ", ")
}
