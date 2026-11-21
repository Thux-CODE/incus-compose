package main

import (
	"cmp"
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

	// errRolloutReplaced is a replica's name that another instance holds now
	// than the one read before: nothing more is done to it.
	errRolloutReplaced = errors.New("is another instance than the one read before")
)

const (
	// rolloutHealthReads is how many reads of a starting replica may fail in
	// a row.
	rolloutHealthReads = 3

	// rolloutScanAttempts is how many times the replicas are all read again,
	// backing off, before a failed or slow read stops the run.
	rolloutScanAttempts = 3

	// rolloutScanFresh is the longest the reads of every replica may take for
	// what they say together to count: a verdict read at the start of a long
	// round may have changed by its end.
	rolloutScanFresh = 10 * time.Second
)

// replicaRead is a replica as Incus has it at the moment it is read.
type replicaRead struct {
	// ID tells the instance from another that took its name since
	// (volatile.uuid).
	ID string

	// Status is its user.healthcheck.status.
	Status  string
	Running bool

	// Drained is the drain ic-dns goes by, profiles included; DrainedBy the
	// rollout run that set it, "" for a drain no rollout set.
	Drained   bool
	DrainedBy string
}

// serves reports whether ic-dns answers the service name with the replica:
// running, not drained and healthy.
func (r replicaRead) serves() bool {
	return r.Running && !r.Drained && r.Status == shared.HealthStatusHealthy
}

// rolloutOps is what a rollout does to the replicas of one service, by index.
type rolloutOps interface {
	// create creates the replica from the compose file, marked as this run's,
	// and starts it, without waiting for its healthcheck. The first one a
	// rollout creates runs the service's pre_start hooks that are not per
	// replica, and is marked as having run them.
	create(ctx context.Context, index int, first bool) error

	// health reads the replica as Incus has it now.
	health(ctx context.Context, index int) (replicaRead, error)

	// drain sets the replica's drain label, marked as this run's, or clears
	// both.
	drain(ctx context.Context, index int, drained bool) error

	// retire stops the replica within its grace period and deletes it. With
	// stopped it was read stopped, and is deleted only while it is.
	retire(ctx context.Context, index int, stopped bool) error

	// discard deletes a new replica that did not become healthy, or with keep
	// leaves it stopped.
	discard(ctx context.Context, index int, keep bool) error

	// unmark takes the marks of the run off a replica it made, once the
	// rollout is over.
	unmark(ctx context.Context, index int) error

	// sleep waits for d, or until ctx is done.
	sleep(ctx context.Context, d time.Duration) error
}

// replica is one index of a service as the rollout sees it.
type replica struct {
	index int

	// id tells the instance from another by its name (volatile.uuid), and
	// created is when it was made.
	id      string
	created time.Time

	// current is made from the service config and the image as they are now:
	// what a create of the rollout would make (rolloutCurrent).
	current bool

	// fresh is a replica the rollout does not replace: one it created, or one
	// an earlier run left up to date (rolloutResumed).
	fresh bool

	running bool
	drained bool

	// drainedBy is the run that drained it, "" for a drain no rollout set;
	// runID the run that made it, and preStart the run whose service-wide
	// pre_start hooks it ran.
	drainedBy string
	runID     string
	preStart  string
}

// rolloutPlan is one service's rollout: what exists, and how long to wait.
type rolloutPlan struct {
	Service  string
	Replicas int
	Present  []replica

	// Taken are the indexes whose names instances not the service's hold,
	// which a new replica skips rather than adopt.
	Taken []int

	// RunID is this run's: the one an interrupted run marked the replicas it
	// made with, picked up again, or a new one.
	RunID string

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
	resumed := rolloutResumed(plan.Present, plan.Replicas, plan.RunID)
	present := make([]string, 0, len(plan.Present))

	for _, r := range plan.Present {
		state := "stopped"
		if r.running {
			state = "running"
		}

		switch {
		case r.drained && r.drainedBy == "":
			state += ", drained by hand"
		case r.drained:
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

// rolloutAdoptedRun returns the run an interrupted rollout marked the replicas
// it made with, when one of them is still what a create makes now: this run
// picks it up. "" when there is none, and the run is a new one.
func rolloutAdoptedRun(present []replica) string {
	for _, r := range present {
		if r.runID != "" && r.current {
			return r.runID
		}
	}

	return ""
}

// rolloutResumed returns the replicas an earlier, interrupted rollout left up
// to date, which this one keeps: marked with this run, current, running and
// not drained. With no replica marked, a surplus of replicas that are all
// current (made by `up`, or by a rollout from before the marks) keeps the
// newest that serve, up to the declared count: the others are what is left to
// retire. Otherwise none is kept, and a rollout run on an unchanged service
// replaces them all, as it is asked to.
func rolloutResumed(present []replica, replicas int, runID string) map[int]bool {
	out := map[int]bool{}

	marked := runID != "" && slices.ContainsFunc(present, func(r replica) bool { return r.runID == runID })
	if marked {
		for _, r := range present {
			if r.runID == runID && r.current && r.running && !r.drained {
				out[r.index] = true
			}
		}

		return out
	}

	if len(present) <= replicas || slices.ContainsFunc(present, func(r replica) bool { return !r.current }) {
		return out
	}

	var serving []replica

	for _, r := range present {
		if r.running && !r.drained {
			serving = append(serving, r)
		}
	}

	slices.SortFunc(serving, func(a, b replica) int {
		return cmp.Or(b.created.Compare(a.created), b.index-a.index)
	})

	for _, r := range serving[:min(replicas, len(serving))] {
		out[r.index] = true
	}

	return out
}

// rollout replaces every replica of a service with one created from the
// compose file, one at a time and new before old: a replica is retired only
// once the one that takes its place reports healthy, and only while the
// declared count serves without it.
//
// A service is its replicas, whatever their indexes, which is also how `up`
// reconciles it. So a new replica takes the lowest free index, and the old one
// it replaces leaves its index free for the next: N creates for N replicas,
// and with one replica `web-1` becomes `web-2`, and `web-1` again on the next
// rollout.
//
// Every replica a run makes carries the run's id, and so does a drain it
// sets: a rollout run again picks up from whatever an earlier one left. The
// replicas that one made are kept once healthy (rolloutResumed), one it left
// drained is removed first when the others serve without it
// (rolloutSettleDrained), and its service-wide pre_start hooks do not run
// again. A drain no rollout set is never taken off. Once the rollout is over
// the marks are taken off the replicas it leaves.
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

	var unmarked error

	for _, i := range slices.Sorted(maps.Keys(slots)) {
		if !slots[i].fresh {
			continue
		}

		err := ops.unmark(context.WithoutCancel(ctx), i)
		if err != nil {
			unmarked = errors.Join(unmarked, fmt.Errorf("%s: taking off the rollout's marks: %w", plan.Name(i), err))
		}
	}

	if unmarked != nil {
		_, _ = fmt.Fprintf(plan.Out, "%s: rolled out, %s, but the marks of the run are left on some replicas: a rollout run again takes them as up to date\n", plan.Service, rolloutState(plan, slots))

		return unmarked
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

	resumed := rolloutResumed(plan.Present, n, plan.RunID)

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

	// The service-wide pre_start hooks run once per rollout: not again when
	// a replica records that they ran for this run, picked up again.
	first := !slices.ContainsFunc(plan.Present, func(r replica) bool {
		return plan.RunID != "" && r.preStart == plan.RunID
	})

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

		slots[index] = &replica{index: index, created: plan.Now(), current: true, fresh: true, running: true, runID: plan.RunID}

		err = ops.create(ctx, index, first)
		if err == nil {
			if first {
				slots[index].preStart = plan.RunID
			}

			_, _ = fmt.Fprintf(plan.Out, "%s: started, waiting up to %s for healthy\n", plan.Name(index), plan.HealthTimeout)

			err = rolloutWaitHealthy(ctx, ops, plan, index)
		}

		first = false

		if err != nil {
			if ctx.Err() != nil {
				err = fmt.Errorf("interrupted before it was healthy: %w", ctx.Err())
			}

			err = fmt.Errorf("%s: %w", plan.Name(index), err)

			// One whose health could not be read may be the only one that
			// serves: it is left as it is, marked as this run's, for a run
			// again to read.
			if errors.Is(err, errRolloutUnread) && ctx.Err() == nil {
				_, _ = fmt.Fprintf(plan.Out, "%s: left as it is, its health could not be read\n", plan.Name(index))

				return err
			}

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
// drained by a rollout. A rollout drains only right before a stop, so such a
// replica was left so by one cut short between its drain and its removal,
// once the one taking its place was healthy. With a replica more than
// declared, and the declared count serving without it (healthy, not drained),
// it stays drained and is the first to go; else it is put back and serves
// again until its turn comes. The others are read now: one this run took as
// up to date may no longer serve, and then the drained one is the service's.
// A drain no rollout set is left as it is: that replica does not serve, and
// is replaced as an old one.
func rolloutSettleDrained(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica) error {
	var drained []*replica

	for _, i := range slices.Sorted(maps.Keys(slots)) {
		r := slots[i]
		if r.running && r.drained && r.drainedBy != "" {
			drained = append(drained, r)
		}
	}

	if len(drained) == 0 {
		return nil
	}

	surplus := len(slots) - plan.Replicas
	serving := 0

	if surplus > 0 {
		reads, err := rolloutScan(ctx, ops, plan, slots)
		if err != nil {
			return err
		}

		for _, read := range reads {
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
// the oldest, then the lowest index, which keeps the indexes a rollout cycles
// through low. It is the order in which `up` gives up a surplus
// (project.reconcileReplicas), with the replicas this run keeps being here no
// candidates.
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
		return cmp.Or(rank(a)-rank(b), a.created.Compare(b.created), a.index-b.index)
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
	reads, err := rolloutScan(ctx, ops, plan, slots)
	if err != nil {
		return err
	}

	healthy := map[int]bool{}

	var lapsed error

	for _, i := range slices.Sorted(maps.Keys(slots)) {
		r := slots[i]
		read := reads[i]
		healthy[i] = read.serves()

		if r.fresh && !healthy[i] {
			r.fresh = false

			lapsed = errors.Join(lapsed, fmt.Errorf("%s: %w (%s), given back", plan.Name(i), errRolloutLapsed, rolloutShown(read)))
		}
	}

	victim := rolloutVictim(slots, healthy)

	err = rolloutRetire(ctx, ops, plan, slots, victim)
	if err != nil {
		return errors.Join(lapsed, err)
	}

	delete(slots, victim)

	return lapsed
}

// rolloutShown says how a replica read, for a message.
func rolloutShown(read replicaRead) string {
	switch {
	case !read.Running:
		return "stopped"
	case read.Drained:
		return "drained"
	case read.Status == "":
		return "not checked yet"
	default:
		return read.Status
	}
}

// rolloutScan reads every replica, all in one round that must be quick for
// what it reads to count together (rolloutScanFresh), and checks each is the
// instance read before. A round that fails or takes too long is made again,
// backing off, up to rolloutScanAttempts times: a hiccup of the API does not
// stop a rollout, but no decision is ever made on a read that failed.
//
// What it reads is the state at its end, give or take its length: ic-healthd
// may call a replica unhealthy at any time, and a stop that follows a round
// shortens that window, it cannot close it.
func rolloutScan(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica) (map[int]replicaRead, error) {
	var last error

	for attempt := 1; ; attempt++ {
		reads, err := rolloutScanOnce(ctx, ops, plan, slots)
		if err == nil {
			return reads, nil
		}

		if ctx.Err() != nil {
			return nil, fmt.Errorf("interrupted: %w", ctx.Err())
		}

		if errors.Is(err, errRolloutReplaced) {
			return nil, err
		}

		last = err

		if attempt >= rolloutScanAttempts {
			return nil, fmt.Errorf("%w after %d rounds: %w", errRolloutUnread, attempt, last)
		}

		err = ops.sleep(ctx, plan.Poll<<(attempt-1))
		if err != nil {
			return nil, fmt.Errorf("interrupted: %w", err)
		}
	}
}

func rolloutScanOnce(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica) (map[int]replicaRead, error) {
	start := plan.Now()
	reads := map[int]replicaRead{}

	for _, i := range slices.Sorted(maps.Keys(slots)) {
		read, err := ops.health(ctx, i)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", plan.Name(i), err)
		}

		id := slots[i].id
		if id != "" && read.ID != "" && read.ID != id {
			return nil, fmt.Errorf("%s: %w", plan.Name(i), errRolloutReplaced)
		}

		reads[i] = read
	}

	took := plan.Now().Sub(start)
	if took > rolloutScanFresh {
		return nil, fmt.Errorf("the reads took %s, more than %s", took.Round(time.Second), rolloutScanFresh)
	}

	for i, read := range reads {
		r := slots[i]
		if r.id == "" {
			r.id = read.ID
		}

		r.running = read.Running
		r.drained = read.Drained
		r.drainedBy = read.DrainedBy
	}

	return reads, nil
}

// rolloutEnough checks, on one round of reads, that without the victim the
// declared count serves.
func rolloutEnough(plan rolloutPlan, reads map[int]replicaRead, victim int) error {
	serving := 0

	for i, read := range reads {
		if i != victim && read.serves() {
			serving++
		}
	}

	if serving < plan.Replicas {
		return fmt.Errorf("%s not retired: %w: %d of the others serve, %d declared", plan.Name(victim), errRolloutShort, serving, plan.Replicas)
	}

	return nil
}

// rolloutRetire drains a running replica, gives clients the grace to move off
// it and retires it. Every replica is read first, as one found stopped may
// run again, and a running one goes only while the declared count serves
// without it: checked before the drain and again, on a round of reads made
// then, right before the stop. One that reads stopped is deleted only while
// it is, with no drain, since it serves nothing.
//
// The stop is put off, the drain taken off and the run failed: when fewer
// would serve, in which case the new replicas of this run that do not serve
// are taken away too, a rollback; on an interrupt before the stop; and on a
// failed drain. A drain no rollout set stays. A replica whose name another
// instance took, or put back by something else during its grace, is left
// alone. Once the stop has begun it is seen through.
func rolloutRetire(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica, victim int) error {
	r := slots[victim]
	name := plan.Name(victim)

	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("interrupted: %w", err)
	}

	reads, err := rolloutScan(ctx, ops, plan, slots)
	if err != nil {
		return fmt.Errorf("%s not retired: %w", name, err)
	}

	if !reads[victim].Running {
		err = ops.retire(context.WithoutCancel(ctx), victim, true)
		if err != nil {
			return fmt.Errorf("%s: retiring: %w", name, err)
		}

		_, _ = fmt.Fprintf(plan.Out, "%s: removed\n", name)

		return nil
	}

	// A drain no rollout set is the operator's: it is neither set again nor
	// taken off.
	byHand := r.drained && r.drainedBy == ""

	putBack := func() error {
		if byHand {
			return nil
		}

		return rolloutUndrain(ctx, ops, plan, r)
	}

	err = rolloutEnough(plan, reads, victim)
	if err != nil {
		return err
	}

	if !byHand {
		err = ops.drain(ctx, victim, true)
		if err != nil {
			return errors.Join(fmt.Errorf("%s: draining: %w", name, err), putBack())
		}

		r.drained = true
		r.drainedBy = plan.RunID
	}

	_, _ = fmt.Fprintf(plan.Out, "%s: drained, waiting %s\n", name, plan.Grace)

	err = ops.sleep(ctx, plan.Grace)
	if err == nil {
		err = ctx.Err()
	}

	if err != nil {
		return errors.Join(fmt.Errorf("interrupted before %s was stopped: %w", name, err), putBack())
	}

	reads, err = rolloutScan(ctx, ops, plan, slots)

	switch {
	case errors.Is(err, errRolloutReplaced):
		return fmt.Errorf("%s not retired: %w", name, err)
	case err != nil && ctx.Err() != nil:
		return errors.Join(fmt.Errorf("interrupted before %s was stopped: %w", name, ctx.Err()), putBack())
	case err != nil:
		return errors.Join(fmt.Errorf("%s not retired: %w", name, err), putBack())
	}

	if !reads[victim].Drained {
		return fmt.Errorf("%s not retired: put back into the service name by something else during its grace", name)
	}

	err = rolloutEnough(plan, reads, victim)
	if err != nil {
		return errors.Join(err, putBack(), rolloutTakeAwayLapsed(ctx, ops, plan, slots, reads, victim))
	}

	err = ops.retire(context.WithoutCancel(ctx), victim, false)
	if err != nil {
		return fmt.Errorf("%s: retiring: %w", name, err)
	}

	_, _ = fmt.Fprintf(plan.Out, "%s: removed\n", name)

	return nil
}

// rolloutTakeAwayLapsed takes away, in a rollback, the replicas this run took
// as new that do not serve on the reads given.
func rolloutTakeAwayLapsed(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica, reads map[int]replicaRead, victim int) error {
	var errs error

	for _, i := range slices.Sorted(maps.Keys(reads)) {
		r := slots[i]
		if i == victim || !r.fresh || reads[i].serves() {
			continue
		}

		r.fresh = false

		err := ops.discard(context.WithoutCancel(ctx), i, plan.KeepFailed)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("taking %s away: %w", plan.Name(i), err))
			continue
		}

		_, _ = fmt.Fprintf(plan.Out, "%s: taken away, it no longer serves (%s)\n", plan.Name(i), rolloutShown(reads[i]))

		r.running = false
		if !plan.KeepFailed {
			delete(slots, i)
		}
	}

	return errs
}

func rolloutUndrain(ctx context.Context, ops rolloutOps, plan rolloutPlan, r *replica) error {
	err := ops.drain(context.WithoutCancel(ctx), r.index, false)
	if err != nil {
		return fmt.Errorf("%s: still drained: %w", plan.Name(r.index), err)
	}

	r.drained = false
	r.drainedBy = ""

	return nil
}

// rolloutWaitHealthy waits for ic-healthd to call the replica healthy. Any
// other verdict ends the wait: unhealthy, a stop, or no verdict in time. A
// verdict read past the timeout, after a read that took long, is no verdict
// in time either.
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

		if status == shared.HealthStatusUnhealthy {
			return fmt.Errorf("%w after %s", errRolloutUnhealthy, elapsed)
		}

		if status == shared.HealthStatusHealthy {
			if elapsed > plan.HealthTimeout {
				return fmt.Errorf("%w (%s): healthy read only after %s", errRolloutTimeout, plan.HealthTimeout, elapsed)
			}

			return nil
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
