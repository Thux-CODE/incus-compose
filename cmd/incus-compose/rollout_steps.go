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

	// errRolloutUnread is a health that could not be read: no verdict, so it
	// neither keeps nor gives up a replica, and no replica is retired on it.
	errRolloutUnread = errors.New("health not read")

	// errRolloutShort is a retire refused: without the replica to retire,
	// fewer than the declared count would serve.
	errRolloutShort = errors.New("fewer replicas would serve than declared")

	// errRolloutLapsed is a replica the rollout counted as new that no longer
	// serves: it is old again and given back, and the run stops.
	errRolloutLapsed = errors.New("no longer serves")
)

// rolloutHealthReads is how many reads of a starting replica may fail in a row.
const rolloutHealthReads = 3

// replicaRead is a replica as Incus has it at the moment it is read.
type replicaRead struct {
	// Status is its user.healthcheck.status.
	Status  string
	Running bool
	Drained bool
}

// serves reports whether ic-dns answers the service name with the replica:
// running, not drained and healthy.
func (r replicaRead) serves() bool {
	return r.Running && !r.Drained && r.Status == shared.HealthStatusHealthy
}

// rolloutOps is what a rollout does to the replicas of one service, by index.
type rolloutOps interface {
	// create creates the replica from the compose file and starts it, without
	// waiting for its healthcheck. The first one a rollout creates runs the
	// service's pre_start hooks that are not per replica.
	create(ctx context.Context, index int, first bool) error

	// health reads the replica's healthcheck status, whether it runs and
	// whether it is drained, as Incus has them now.
	health(ctx context.Context, index int) (replicaRead, error)

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

	// Taken are the indexes whose names instances not the service's hold,
	// which a new replica skips rather than adopt.
	Taken []int

	// Name returns the instance name of an index.
	Name func(index int) string

	// KeepFailed leaves a new replica that failed stopped, for a look at it.
	KeepFailed bool

	HealthTimeout time.Duration
	Grace         time.Duration
	Poll          time.Duration
	Now           func() time.Time
	Out           io.Writer

	// Notes are what a dry run could not check, which its plan says.
	Notes []string
}

// rolloutDescribe prints what a rollout of the plan would do, for --dry-run.
// A replica an earlier run left up to date reads "up to date" and takes no
// create, so a rollout picked up again may take fewer creates than replicas,
// none included.
func rolloutDescribe(plan rolloutPlan) {
	resumed := rolloutResumed(plan.Present, plan.Replicas)
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

	for _, note := range plan.Notes {
		_, _ = fmt.Fprintf(plan.Out, "%s: not checked: %s\n", plan.Service, note)
	}
}

// rolloutResumed returns the replicas an earlier, interrupted rollout left up
// to date, which this one keeps: current, running and not drained, next to at
// least one that is not current. When all are current nothing tells the ones
// an earlier run made from the ones it was to replace, and a rollout run on an
// unchanged service is asked to replace them all, so none is kept; but for one
// replica declared and one left running drained: only a rollout drains, right
// before the stop and once the replica taking its place is healthy, so the
// other is that one, and what is left is to retire the drained one.
func rolloutResumed(present []replica, replicas int) map[int]bool {
	out := map[int]bool{}

	if !slices.ContainsFunc(present, func(r replica) bool { return !r.current }) {
		drained := slices.ContainsFunc(present, func(r replica) bool { return r.running && r.drained })
		if replicas != 1 || !drained {
			return out
		}
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
	// back the unhealthy one first, and the run stops before a create. A
	// health that cannot be read is no verdict: the run stops there, and
	// retires nothing on it.
	var failed error

	resumed := rolloutResumed(plan.Present, n)

	for _, i := range slices.Sorted(maps.Keys(resumed)) {
		r := slots[i]
		r.fresh = true

		_, _ = fmt.Fprintf(plan.Out, "%s: up to date, left by an earlier rollout; waiting up to %s for healthy\n", plan.Name(i), plan.HealthTimeout)

		err := rolloutWaitHealthy(ctx, ops, plan, i)
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("interrupted: %w", ctx.Err())
			}

			if errors.Is(err, errRolloutUnread) {
				return errors.Join(failed, fmt.Errorf("%s: %w", plan.Name(i), err))
			}

			r.fresh = false
			failed = errors.Join(failed, fmt.Errorf("%s: %w", plan.Name(i), err))
		}
	}

	err := rolloutSettleDrained(ctx, ops, plan, slots)
	if err != nil {
		return errors.Join(failed, err)
	}

	// The service-wide pre_start hooks ran with the first replica an earlier
	// run made, when this one keeps any: once per rollout, picked up or not.
	first := len(resumed) == 0

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
			// One replica more than declared: the one in flight once it is
			// healthy, or one an earlier run cut short left. One goes before
			// another is created, so there are never two more.
			err := rolloutGiveBack(ctx, ops, plan, slots)
			if err != nil {
				return errors.Join(failed, err)
			}

			continue
		}

		if failed != nil {
			return failed
		}

		index := rolloutFree(slots, plan.Taken)

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

		// With fewer replicas than declared there this one adds to them;
		// else the surplus above retires one, on what it reads then.
	}
}

// rolloutSettleDrained decides what becomes of the replicas found running
// drained. Only a rollout drains, and only right before a stop, so such a
// replica was left so by one cut short between its drain and its removal,
// once the one taking its place was healthy. With a replica more than
// declared, and the declared count serving without it (healthy, not drained),
// it stays drained and is the first to go; else it is put back and serves
// again until its turn comes. The others are read now: one this run took as
// up to date may no longer serve, and then the drained one is the service's.
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

	if surplus > 0 {
		for _, i := range slices.Sorted(maps.Keys(slots)) {
			r := slots[i]
			if r.running && r.drained {
				continue
			}

			read, err := ops.health(ctx, i)
			if err != nil {
				return fmt.Errorf("%s: %w: %w", plan.Name(i), errRolloutUnread, err)
			}

			r.running = read.Running
			r.drained = read.Drained

			if read.serves() {
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

		if surplus > 0 {
			_, _ = fmt.Fprintf(plan.Out, "%s: drained by an earlier rollout, put back: without it %d of %d declared would serve\n", plan.Name(r.index), serving, plan.Replicas)
		} else {
			_, _ = fmt.Fprintf(plan.Out, "%s: drained by an earlier rollout, put back\n", plan.Name(r.index))
		}
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
func rolloutFree(slots map[int]*replica, taken []int) int {
	for i := 1; ; i++ {
		_, held := slots[i]
		if !held && !slices.Contains(taken, i) {
			return i
		}
	}
}

// rolloutVictim picks the old replica to retire, of which there is at least
// one: one that does not run, is drained already or, where healthy says, is
// not healthy goes first, then one made from an older config or image, then
// the lowest index, which keeps the indexes a rollout cycles through low. It
// is the order in which `up` gives up a surplus (project.reconcileReplicas),
// with the replicas this run keeps being here no candidates, and the lowest
// index going where `up` drops the highest.
func rolloutVictim(slots map[int]*replica, healthy map[int]bool) int {
	rank := func(r *replica) int {
		switch {
		case !r.running:
			return 0
		case r.drained:
			return 1
		case healthy != nil && !healthy[r.index]:
			return 2
		case !r.current:
			return 3
		default:
			return 4
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

// rolloutGiveBack retires one old replica of more than declared: the old one
// a healthy new one replaces, or one an earlier run left over. Every replica
// is read first, and the one to go is picked on what they read now, so a
// healthy one goes only when the others are healthy too. A replica this run
// took as new that no longer serves is old again: it is the one given back,
// and the run stops after, with an error that says so.
func rolloutGiveBack(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica) error {
	healthy := map[int]bool{}

	var lapsed error

	for _, i := range slices.Sorted(maps.Keys(slots)) {
		r := slots[i]

		read, err := ops.health(ctx, i)
		if err != nil {
			return fmt.Errorf("%s: %w: %w", plan.Name(i), errRolloutUnread, err)
		}

		r.running = read.Running
		r.drained = read.Drained
		healthy[i] = read.serves()

		if r.fresh && !healthy[i] {
			r.fresh = false

			shown := read.Status
			if !read.Running {
				shown = "stopped"
			} else if read.Drained {
				shown = "drained"
			}

			lapsed = errors.Join(lapsed, fmt.Errorf("%s: %w (%s), given back", plan.Name(i), errRolloutLapsed, shown))
		}
	}

	victim := rolloutVictim(slots, healthy)

	err := rolloutRetire(ctx, ops, plan, slots, victim)
	if err != nil {
		return errors.Join(lapsed, err)
	}

	delete(slots, victim)

	return lapsed
}

// rolloutServing reads every replica but the one about to be retired, now,
// and counts the ones that serve: running, not drained and healthy. A read
// that fails is an error, never a count: no retire goes ahead on it.
func rolloutServing(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica, victim int) (int, error) {
	serving := 0

	for _, i := range slices.Sorted(maps.Keys(slots)) {
		if i == victim {
			continue
		}

		read, err := ops.health(ctx, i)
		if err != nil {
			return 0, fmt.Errorf("%s: %w: %w", plan.Name(i), errRolloutUnread, err)
		}

		slots[i].running = read.Running
		slots[i].drained = read.Drained

		if read.serves() {
			serving++
		}
	}

	return serving, nil
}

// rolloutEnough is the check a running replica passes before its drain and
// again before its stop: without it the declared count serves, read now.
func rolloutEnough(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica, victim int) error {
	serving, err := rolloutServing(ctx, ops, plan, slots, victim)
	if err != nil {
		return fmt.Errorf("%s not retired: %w", plan.Name(victim), err)
	}

	if serving < plan.Replicas {
		return fmt.Errorf("%s not retired: %w: %d of the others serve, %d declared", plan.Name(victim), errRolloutShort, serving, plan.Replicas)
	}

	return nil
}

// rolloutRetire drains a running replica, gives clients the grace to move off
// it and retires it. It is read again first, as one found stopped may run
// again, and a running one goes only while the declared count serves without
// it: checked before the drain and again before the stop, which is put off
// otherwise, and the replica put back. An interrupt during the grace puts it
// back too; once the stop has begun it is seen through. A replica found
// drained, by an earlier run, is left drained whatever happens.
func rolloutRetire(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica, victim int) error {
	r := slots[victim]
	name := plan.Name(victim)

	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("interrupted: %w", err)
	}

	read, err := ops.health(ctx, victim)
	if err != nil {
		return fmt.Errorf("%s not retired: %w: %w", name, errRolloutUnread, err)
	}

	r.running = read.Running
	r.drained = read.Drained

	if r.running {
		putBack := func() error { return nil }
		if !r.drained {
			putBack = func() error { return rolloutUndrain(ctx, ops, plan, r) }
		}

		err := rolloutEnough(ctx, ops, plan, slots, victim)
		if err != nil {
			return err
		}

		err = ops.drain(ctx, victim, true)
		if err != nil {
			return errors.Join(fmt.Errorf("%s: draining: %w", name, err), putBack())
		}

		r.drained = true

		_, _ = fmt.Fprintf(plan.Out, "%s: drained, waiting %s\n", name, plan.Grace)

		err = ops.sleep(ctx, plan.Grace)
		if err != nil {
			return errors.Join(fmt.Errorf("interrupted: %w", err), putBack())
		}

		// The grace is over and the stop begins, which an interrupt from here
		// on does not hold up.
		err = rolloutEnough(context.WithoutCancel(ctx), ops, plan, slots, victim)
		if err != nil {
			return errors.Join(err, putBack())
		}
	}

	err = ops.retire(context.WithoutCancel(ctx), victim)
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
		read, err := ops.health(ctx, index)
		if err != nil {
			// A wait of minutes outlives a hiccup of the API.
			failed++
			if ctx.Err() != nil {
				return err
			}

			if failed >= rolloutHealthReads {
				return fmt.Errorf("%w: %w", errRolloutUnread, err)
			}

			err = ops.sleep(ctx, plan.Poll)
			if err != nil {
				return fmt.Errorf("interrupted: %w", err)
			}

			continue
		}

		failed = 0

		status := read.Status
		elapsed := plan.Now().Sub(start).Round(time.Second)

		if !read.Running {
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
