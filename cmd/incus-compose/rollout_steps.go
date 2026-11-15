package main

import (
	"context"
	"errors"
	"fmt"
	"io"
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

// rolloutOps is what a rollout does to the replicas of one service, by index.
type rolloutOps interface {
	// create creates the replica from the compose file and starts it, without
	// waiting for its healthcheck.
	create(ctx context.Context, index int) error

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
	index   int
	fresh   bool
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

// rollout replaces every replica of a service with one created from the
// compose file, one at a time and new before old: a replica is retired only
// once the one that takes its place reports healthy.
//
// The names stay `<service>-1..N`, which is what `up` reconciles to. A rollout
// has one name more to work with, N+1, so it takes N+1 creates: the first new
// replica is the spare N+1, each one after it takes the name of the old one
// retired before it, and the spare goes last.
//
// A new replica that fails, or a rollout interrupted before it is healthy,
// is taken away again and leaves the old ones as they were. A rollout run
// again picks up from whatever an earlier one left.
func rollout(ctx context.Context, ops rolloutOps, plan rolloutPlan) error {
	slots := map[int]*replica{}
	for _, r := range plan.Present {
		slots[r.index] = &r
	}

	err := rolloutSteps(ctx, ops, plan, slots)
	if err != nil {
		_, _ = fmt.Fprintf(plan.Out, "%s: stopped, left %s\n", plan.Service, rolloutState(plan, slots))
		return err
	}

	_, _ = fmt.Fprintf(plan.Out, "%s: rolled out, %s\n", plan.Service, rolloutState(plan, slots))

	return nil
}

func rolloutSteps(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica) error {
	n := plan.Replicas

	for {
		if rolloutDone(slots, n) {
			return nil
		}

		err := ctx.Err()
		if err != nil {
			return fmt.Errorf("interrupted: %w", err)
		}

		index, ok := rolloutFree(slots, n)
		if !ok {
			// Every name up to the spare is taken, by an earlier run cut short:
			// one has to go before there is a name to create.
			err := rolloutGiveBack(ctx, ops, plan, slots)
			if err != nil {
				return err
			}

			continue
		}

		_, _ = fmt.Fprintf(plan.Out, "%s: creating\n", plan.Name(index))

		slots[index] = &replica{index: index, fresh: true, running: true}

		err = ops.create(ctx, index)
		if err == nil {
			err = rolloutWaitHealthy(ctx, ops, plan, index)
		}

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

		victim, ok := rolloutVictim(slots, n, index, nil)
		if !ok {
			continue
		}

		err = rolloutRetire(ctx, ops, plan, slots[victim])
		if err != nil {
			return err
		}

		delete(slots, victim)
	}
}

// rolloutDone reports whether every name up to n holds a new replica and the
// spare is gone.
func rolloutDone(slots map[int]*replica, n int) bool {
	for i := 1; i <= n; i++ {
		r, ok := slots[i]
		if !ok || !r.fresh {
			return false
		}
	}

	_, spare := slots[n+1]

	return !spare
}

// rolloutFree returns the lowest free name up to n, else the spare n+1.
func rolloutFree(slots map[int]*replica, n int) (int, bool) {
	for i := 1; i <= n+1; i++ {
		_, taken := slots[i]
		if !taken {
			return i, true
		}
	}

	return 0, false
}

// rolloutVictim picks the replica to retire now that index is healthy: an old
// one if there is any, else the new spare. Among the old, one that does not
// run, is drained already or, where healthy says, is not healthy goes first,
// then the lowest index, whose name the next new replica takes.
func rolloutVictim(slots map[int]*replica, n, index int, healthy map[int]bool) (int, bool) {
	var old []*replica
	for _, r := range slots {
		if r.index != index && !r.fresh {
			old = append(old, r)
		}
	}

	if len(old) == 0 {
		spare, ok := slots[n+1]
		if ok && spare.index != index {
			return spare.index, true
		}

		return 0, false
	}

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

	victim := slices.MinFunc(old, func(a, b *replica) int {
		d := rank(a) - rank(b)
		if d != 0 {
			return d
		}

		return a.index - b.index
	})

	return victim.index, true
}

// rolloutGiveBack retires one of n+1 old replicas before anything new exists.
// A healthy one goes only when all the others are healthy too, so the service
// keeps n replicas serving.
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

	victim, _ := rolloutVictim(slots, plan.Replicas, 0, healthy)

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
		err := ops.drain(ctx, r.index, true)
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

	_, _ = fmt.Fprintf(plan.Out, "%s: started, waiting up to %s for healthy\n", plan.Name(index), plan.HealthTimeout)

	for {
		status, running, err := ops.health(ctx, index)
		if err != nil {
			return err
		}

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
