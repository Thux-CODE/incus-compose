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

	// errRolloutStale is a stop not begun, as the round it went by was too
	// old by then; errRolloutRestarted a victim ic-healthd started again
	// after its stop. Either takes a new round.
	errRolloutStale     = errors.New("the round the stop went by is no longer fresh")
	errRolloutRestarted = errors.New("started again")
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

	// rolloutReadTimeout bounds each read of a replica, which the transport
	// would let wait up to an hour.
	rolloutReadTimeout = 30 * time.Second

	// rolloutStopFresh is how long after the end of the round it goes by a
	// stop may still be sent: the identity read and the stop request fit in
	// it, the health-check marker being written before the round.
	rolloutStopFresh = 15 * time.Second
)

// rolloutRead reads a replica, within rolloutReadTimeout.
func rolloutRead(ctx context.Context, ops rolloutOps, index int) (replicaRead, error) {
	ctx, cancel := context.WithTimeout(ctx, rolloutReadTimeout)
	defer cancel()

	return ops.health(ctx, index)
}

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
	// and starts it, without waiting for its healthcheck; it returns what
	// tells the instance from another by its name. The first one a rollout
	// creates runs the service's pre_start hooks that are not per replica,
	// and is marked as having run them.
	create(ctx context.Context, index int, first bool) (string, error)

	// health reads the replica as Incus has it now.
	health(ctx context.Context, index int) (replicaRead, error)

	// drain sets the replica's drain label, marked as this run's, or clears
	// both.
	drain(ctx context.Context, index int, drained bool) error

	// retire stops the replica within its grace period and deletes it. With
	// stopped it was read stopped, and is deleted only while it is.
	//
	// A running one is stopped only up to notAfter, when the round the stop
	// goes by is still fresh (errRolloutStale), and once: one that ic-healthd
	// starts again after its stop is left for a new round
	// (errRolloutRestarted).
	//
	// With killAfterStop, the try follows one that found it started again
	// after its stop, and the round it goes by was made since: a victim that
	// reads running again after a graceful stop that went through is then
	// killed, within notAfter, as one whose stop ran out is.
	retire(ctx context.Context, index int, stopped bool, notAfter time.Time, killAfterStop bool) error

	// discard deletes a new replica that did not become healthy, or with keep
	// leaves it stopped.
	discard(ctx context.Context, index int, keep bool) error

	// unmark takes the marks of the run off a replica it made, once the
	// rollout is over.
	unmark(ctx context.Context, index int) error

	// stopping sets or clears user.healthcheck.stopped on a replica about to
	// be stopped, which keeps ic-healthd's checks and restart policy off it.
	stopping(ctx context.Context, index int, on bool) error

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

	// healthy was called so when the plan was made.
	healthy bool
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

	// PreStart tells a service with service-wide pre_start hooks: a run
	// picked up again keeps what it made only once a replica records they
	// ran, as they run with a create.
	PreStart bool

	// Name returns the instance name of an index.
	Name func(index int) string

	// KeepFailed leaves a new replica that failed stopped, for a look at it.
	KeepFailed bool

	HealthTimeout time.Duration
	Grace         time.Duration
	Poll          time.Duration
	Now           func() time.Time
	Out           io.Writer

	// GraceStep is how often the replicas are read during a grace, 0 for not
	// at all (rolloutGrace).
	GraceStep time.Duration

	// Notes are what a dry run could not check, which its plan says.
	Notes []string
}

// rolloutDescribe prints what a rollout of the plan would do, for --dry-run.
// A replica an earlier run left up to date reads "up to date" and takes no
// create, so a rollout picked up again may take fewer creates than replicas,
// none included.
func rolloutDescribe(plan rolloutPlan) {
	resumed := rolloutResumed(plan)
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

	// A stopped replica with a run's mark is a new one an earlier run left
	// failed: the run takes it away first (rolloutVictim, rolloutRetire).
	for _, r := range plan.Present {
		if !r.running && r.runID != "" && !resumed[r.index] {
			_, _ = fmt.Fprintf(plan.Out, "%s: stopped, left by an earlier run: removed first\n", plan.Name(r.index))
		}
	}

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
//
// Either way no more than the declared count is kept, the healthy and the
// newest first; and with service-wide pre_start hooks the marked ones are kept
// only once one records the hooks ran, which they do with a create.
func rolloutResumed(plan rolloutPlan) map[int]bool {
	present, replicas, runID := plan.Present, plan.Replicas, plan.RunID

	var keep []replica

	marked := runID != "" && slices.ContainsFunc(present, func(r replica) bool { return r.runID == runID })

	switch {
	case marked:
		ran := slices.ContainsFunc(present, func(r replica) bool { return r.preStart == runID })
		if plan.PreStart && !ran {
			return map[int]bool{}
		}

		for _, r := range present {
			if r.runID == runID && r.current && r.running && !r.drained {
				keep = append(keep, r)
			}
		}
	case len(present) > replicas && !slices.ContainsFunc(present, func(r replica) bool { return !r.current }):
		for _, r := range present {
			if r.running && !r.drained {
				keep = append(keep, r)
			}
		}
	}

	slices.SortFunc(keep, func(a, b replica) int {
		healthy := func(r replica) int {
			if r.healthy {
				return 0
			}

			return 1
		}

		return cmp.Or(healthy(a)-healthy(b), b.created.Compare(a.created), b.index-a.index)
	})

	out := map[int]bool{}
	for _, r := range keep[:min(replicas, len(keep))] {
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

	resumed := rolloutResumed(plan)

	for _, i := range slices.Sorted(maps.Keys(resumed)) {
		r := slots[i]
		r.fresh = true

		_, _ = fmt.Fprintf(plan.Out, "%s: up to date, left by an earlier rollout; waiting up to %s for healthy\n", plan.Name(i), plan.HealthTimeout)

		err := rolloutWaitHealthy(ctx, ops, plan, i, r.id)
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("interrupted: %w", ctx.Err())
			}

			if errors.Is(err, errRolloutUnread) || errors.Is(err, errRolloutReplaced) {
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
			if failed != nil {
				return failed
			}

			// Done only if what the run leaves serves, read now: a replica
			// it kept may have turned unhealthy while another was waited for.
			return rolloutCheckServing(ctx, ops, plan, slots)
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

		slots[index] = &replica{index: index, created: plan.Now(), current: true, fresh: true, running: true, runID: plan.RunID}

		id, err := ops.create(ctx, index, first)

		started := err == nil
		if started {
			slots[index].id = id

			if first {
				slots[index].preStart = plan.RunID
			}

			_, _ = fmt.Fprintf(plan.Out, "%s: started, waiting up to %s for healthy\n", plan.Name(index), plan.HealthTimeout)

			err = rolloutWaitHealthy(ctx, ops, plan, index, id)
		}

		first = false

		if err != nil {
			if ctx.Err() != nil {
				err = fmt.Errorf("interrupted before it was healthy: %w", ctx.Err())
			}

			err = fmt.Errorf("%s: %w", plan.Name(index), err)

			// One whose health could not be read may be the only one that
			// serves: it is left as it is, marked as this run's, for a run
			// again to read. One whose name another instance holds now is not
			// this run's to take away.
			if errors.Is(err, errRolloutUnread) && ctx.Err() == nil || errors.Is(err, errRolloutReplaced) {
				_, _ = fmt.Fprintf(plan.Out, "%s: left as it is\n", plan.Name(index))

				return err
			}

			// Its health checking stopped right at the verdict, before the
			// reads and the take-down: else ic-healthd's restart policy acts
			// on the same unhealthy verdict, and its restart holds the
			// instance busy as it is to be deleted.
			if started {
				stopErr := ops.stopping(context.WithoutCancel(ctx), index, true)
				if stopErr != nil {
					err = errors.Join(err, fmt.Errorf("%s: stopping its health checks: %w", plan.Name(index), stopErr))
				}
			}

			// Nor is one taken away that serves while the others do not:
			// healthy too late, or interrupted, as the old ones stopped.
			if started && rolloutNeeded(ctx, ops, plan, slots, index) {
				_, _ = fmt.Fprintf(plan.Out, "%s: left as it is, it serves and the others do not serve the declared count without it\n", plan.Name(index))

				return errors.Join(err, ops.stopping(context.WithoutCancel(ctx), index, false))
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

		// A drain an operator took over since the plan was made is theirs.
		drained = slices.DeleteFunc(drained, func(r *replica) bool { return r.drainedBy == "" })
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

	// Of the ones not running, a failed new one of an earlier run, which
	// carries its mark, goes before an old one.
	failedNew := func(r *replica) int {
		if !r.running && r.runID != "" {
			return 0
		}

		return 1
	}

	victim := slices.MinFunc(rolloutOld(slots), func(a, b *replica) int {
		return cmp.Or(rank(a)-rank(b), failedNew(a)-failedNew(b), a.created.Compare(b.created), a.index-b.index)
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
	reads, _, err := rolloutScanAt(ctx, ops, plan, slots)

	return reads, err
}

// rolloutScanAt is rolloutScan, which also returns when the round it read
// ended, itself no longer than rolloutScanFresh: what it read is fresh from
// then on, not from when it was handed on.
func rolloutScanAt(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica) (map[int]replicaRead, time.Time, error) {
	var last error

	for attempt := 1; ; attempt++ {
		reads, start, err := rolloutScanOnce(ctx, ops, plan, slots)
		if err == nil {
			return reads, start, nil
		}

		if ctx.Err() != nil {
			return nil, time.Time{}, fmt.Errorf("interrupted: %w", ctx.Err())
		}

		if errors.Is(err, errRolloutReplaced) {
			return nil, time.Time{}, err
		}

		last = err

		if attempt >= rolloutScanAttempts {
			return nil, time.Time{}, fmt.Errorf("%w after %d rounds: %w", errRolloutUnread, attempt, last)
		}

		err = ops.sleep(ctx, plan.Poll<<(attempt-1))
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("interrupted: %w", err)
		}
	}
}

func rolloutScanOnce(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica) (map[int]replicaRead, time.Time, error) {
	start := plan.Now()
	reads := map[int]replicaRead{}

	for _, i := range slices.Sorted(maps.Keys(slots)) {
		read, err := rolloutRead(ctx, ops, i)
		if err != nil {
			return nil, start, fmt.Errorf("%s: %w", plan.Name(i), err)
		}

		id := slots[i].id
		if id != "" && read.ID != "" && read.ID != id {
			return nil, start, fmt.Errorf("%s: %w", plan.Name(i), errRolloutReplaced)
		}

		reads[i] = read
	}

	end := plan.Now()

	took := end.Sub(start)
	if took > rolloutScanFresh {
		return nil, end, fmt.Errorf("the reads took %s, more than %s", took.Round(time.Second), rolloutScanFresh)
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

	return reads, end, nil
}

// rolloutRunServes reports whether a replica this run made or kept serves on
// the reads given.
func rolloutRunServes(plan rolloutPlan, slots map[int]*replica, reads map[int]replicaRead) bool {
	for i, read := range reads {
		r := slots[i]
		if r.fresh && r.runID == plan.RunID && read.serves() {
			return true
		}
	}

	return false
}

// rolloutNeeded reports whether a new replica that failed is not to be taken
// away: it serves, and without it fewer than the declared count would. A
// round that cannot be read keeps it too.
func rolloutNeeded(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica, index int) bool {
	reads, _, err := rolloutScanOnce(context.WithoutCancel(ctx), ops, plan, slots)
	if err != nil {
		return true
	}

	if !reads[index].serves() {
		return false
	}

	return rolloutEnough(plan, reads, index) != nil
}

// rolloutCheckServing reads what the run leaves, at its end: a replica it
// made or kept that no longer serves fails the run, which then ends with
// fewer serving than declared.
func rolloutCheckServing(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica) error {
	reads, err := rolloutScan(context.WithoutCancel(ctx), ops, plan, slots)
	if err != nil {
		return err
	}

	var lapsed error

	for _, i := range slices.Sorted(maps.Keys(reads)) {
		if slots[i].fresh && !reads[i].serves() {
			lapsed = errors.Join(lapsed, fmt.Errorf("%s: %w (%s) at the end of the rollout", plan.Name(i), errRolloutLapsed, rolloutShown(reads[i])))
		}
	}

	return lapsed
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
		// It serves nothing, but ic-healthd may start it again: with fewer
		// than declared serving without it, it goes only for a replica of
		// this run that serves in the same round, which a service all down
		// is repaired with. Else it is kept as it is, restart policy and all.
		// Kept, it is put back into the service name when a rollout drained
		// it, to serve once restarted, and the new replicas that do not
		// serve go, so a run again creates afresh: a rollback.
		//
		// One that carries a run's mark is a new replica that failed in an
		// earlier run, not an old one to keep: it goes as the new ones that
		// fail do.
		if r.runID == "" && rolloutEnough(plan, reads, victim) != nil && !rolloutRunServes(plan, slots, reads) {
			var undrain error
			if r.drained && r.drainedBy != "" {
				undrain = rolloutUndrain(ctx, ops, plan, r)
			}

			return errors.Join(fmt.Errorf("%s not retired: it is stopped, fewer than %d serve, and no replica of this run serves in its place", name, plan.Replicas),
				undrain, rolloutTakeAwayLapsed(ctx, ops, plan, slots, reads, victim))
		}

		err = ops.retire(context.WithoutCancel(ctx), victim, true, time.Time{}, false)
		if err != nil {
			return fmt.Errorf("%s: retiring: %w", name, err)
		}

		_, _ = fmt.Fprintf(plan.Out, "%s: removed\n", name)

		return nil
	}

	// A drain no rollout set is the operator's: it is neither set again nor
	// taken off. What a drain is goes by the last read: one an operator took
	// over during the grace is theirs too.
	byHand := r.drained && r.drainedBy == ""

	// The undrain takes the health-check marker off too; with a drain that is
	// not the rollout's, the marker alone is taken off.
	stoppingSet := false

	putBack := func() error {
		if !r.drained || r.drainedBy == "" {
			if stoppingSet {
				return ops.stopping(context.WithoutCancel(ctx), victim, false)
			}

			return nil
		}

		return rolloutUndrain(ctx, ops, plan, r)
	}

	// Short already: an earlier run's drain on it is taken off, so it serves
	// with the others while the run stops.
	err = rolloutEnough(plan, reads, victim)
	if err != nil {
		return errors.Join(err, putBack())
	}

	if !byHand {
		err = ops.drain(ctx, victim, true)
		if err != nil {
			// The drain may have landed though the call failed.
			return errors.Join(fmt.Errorf("%s: draining: %w", name, err), rolloutUndrain(ctx, ops, plan, r))
		}

		r.drained = true
		r.drainedBy = plan.RunID
	}

	_, _ = fmt.Fprintf(plan.Out, "%s: drained, waiting %s\n", name, plan.Grace)

	graceReads, err := rolloutGrace(ctx, ops, plan, slots, victim)

	switch {
	case errors.Is(err, errRolloutShort):
		// The others stopped serving during the grace: the old one goes back
		// at once, not at its end.
		return errors.Join(err, putBack(), rolloutTakeAwayLapsed(ctx, ops, plan, slots, graceReads, victim))
	case errors.Is(err, errRolloutReplaced):
		return fmt.Errorf("%s not retired: %w", name, err)
	case err == nil:
		err = ctx.Err()
	}

	if err != nil {
		return errors.Join(fmt.Errorf("interrupted before %s was stopped: %w", name, err), putBack())
	}

	// Its health checking stopped before the last round, not after it: the
	// write takes seconds, which the stop's freshness has no room for, and it
	// ends ic-healthd's execs on it, which the stop otherwise waits out. It
	// is drained already, so this changes nothing it serves; every way back
	// takes the marker off again.
	err = ops.stopping(context.WithoutCancel(ctx), victim, true)
	if err != nil {
		return errors.Join(fmt.Errorf("%s not retired: stopping its health checks: %w", name, err), putBack())
	}

	stoppingSet = true

	// The stop goes by the round right before it. A stop that would begin
	// later than a round is fresh for (errRolloutStale), or a victim that
	// ic-healthd started again after its stop (errRolloutRestarted), takes a
	// new round before another try.
	//
	// A try put off is kept with whatever went wrong with it, the restart
	// policy put back included, and the last one put off puts the victim
	// back: no stop went through.
	//
	// Once a try found the victim started again after a graceful stop, the
	// next round is the fresh decision a kill may go by: as when Stop returns
	// on a passing Error read with the instance still running.
	var putOff error

	killAfterStop := false

	for attempt := 1; ; attempt++ {
		err = rolloutFinalRetireKill(ctx, ops, plan, slots, victim, byHand, putBack, killAfterStop)
		if errors.Is(err, errRolloutRestarted) {
			killAfterStop = true
		}

		again := errors.Is(err, errRolloutStale) || errors.Is(err, errRolloutRestarted)

		switch {
		case err == nil:
			return nil
		case !again:
			return errors.Join(putOff, err)
		}

		putOff = errors.Join(putOff, err)

		if attempt >= rolloutScanAttempts {
			return errors.Join(fmt.Errorf("%s not retired after %d tries", name, attempt), putOff, putBack())
		}

		_, _ = fmt.Fprintf(plan.Out, "%s: read again before its stop: %v\n", name, err)
	}
}

// rolloutFinalRetire is the last round of reads before a drained victim's
// stop, and the stop if the round allows it.
func rolloutFinalRetire(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica, victim int, byHand bool, putBack func() error) error {
	return rolloutFinalRetireKill(ctx, ops, plan, slots, victim, byHand, putBack, false)
}

// rolloutFinalRetireKill is rolloutFinalRetire, for a try that may kill a
// victim read running again after a graceful stop (rolloutOps.retire).
func rolloutFinalRetireKill(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica, victim int, byHand bool, putBack func() error, killAfterStop bool) error {
	r := slots[victim]
	name := plan.Name(victim)

	reads, readAt, err := rolloutScanAt(ctx, ops, plan, slots)

	switch {
	case errors.Is(err, errRolloutReplaced):
		// Another replica's name changed hands; the victim, still the one
		// read before, is put back. One that changed hands itself is not.
		read, readErr := rolloutRead(context.WithoutCancel(ctx), ops, victim)
		if readErr == nil && (r.id == "" || read.ID == r.id) {
			return errors.Join(fmt.Errorf("%s not retired: %w", name, err), putBack())
		}

		return fmt.Errorf("%s not retired: %w", name, err)
	case err == nil && ctx.Err() != nil, err != nil && ctx.Err() != nil:
		// An interrupt during the last round, even one that read through.
		return errors.Join(fmt.Errorf("interrupted before %s was stopped: %w", name, ctx.Err()), putBack())
	case err != nil:
		return errors.Join(fmt.Errorf("%s not retired: %w", name, err), putBack())
	}

	// The grace counts only spent drained by this run all along: a drain
	// taken off and set again, by hand, may have let it be published again.
	if !reads[victim].Drained || reads[victim].DrainedBy != plan.RunID && !byHand {
		return fmt.Errorf("%s not retired: its drain changed during its grace, by something else than this rollout", name)
	}

	err = rolloutEnough(plan, reads, victim)
	if err != nil {
		return errors.Join(err, putBack(), rolloutTakeAwayLapsed(ctx, ops, plan, slots, reads, victim))
	}

	// The interrupt is seen up to the stop being sent, which the retire
	// checks; an interrupt that stopped nothing puts the victim back.
	err = ops.retire(ctx, victim, false, readAt.Add(rolloutStopFresh), killAfterStop)
	if err != nil && ctx.Err() != nil && errors.Is(err, context.Canceled) {
		return errors.Join(fmt.Errorf("interrupted before %s was stopped: %w", name, err), putBack())
	}

	if err != nil {
		return fmt.Errorf("%s: retiring: %w", name, err)
	}

	_, _ = fmt.Fprintf(plan.Out, "%s: removed\n", name)

	return nil
}

// rolloutGrace waits the grace of a drained victim, reading the others every
// GraceStep: the first round read with fewer than the declared count serving
// without the victim ends it with errRolloutShort and that round's reads, so
// the old replica is put back then, not at the end of the grace. A round that
// cannot be read is no reason for a rollback, and authorizes nothing: the
// check right before the stop has to pass still. With no step, or one as long
// as the grace, it is a single wait.
func rolloutGrace(ctx context.Context, ops rolloutOps, plan rolloutPlan, slots map[int]*replica, victim int) (map[int]replicaRead, error) {
	if plan.GraceStep <= 0 || plan.GraceStep >= plan.Grace {
		return nil, ops.sleep(ctx, plan.Grace)
	}

	start := plan.Now()

	for {
		left := plan.Grace - plan.Now().Sub(start)
		if left <= 0 {
			return nil, nil
		}

		err := ops.sleep(ctx, min(plan.GraceStep, left))
		if err != nil {
			return nil, err
		}

		// The last step ends in the check before the stop.
		if plan.Now().Sub(start) >= plan.Grace {
			return nil, nil
		}

		reads, err := rolloutScan(ctx, ops, plan, slots)

		switch {
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case errors.Is(err, errRolloutReplaced):
			return nil, err
		case err != nil:
			_, _ = fmt.Fprintf(plan.Out, "%s: the replicas could not be read during its grace, which goes on: %v\n", plan.Name(victim), err)

			continue
		}

		err = rolloutEnough(plan, reads, victim)
		if err != nil {
			return reads, fmt.Errorf("during its grace: %w", err)
		}
	}
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
//
// id is the instance the replica is, "" for not known: a read of another one
// by its name ends the wait.
func rolloutWaitHealthy(ctx context.Context, ops rolloutOps, plan rolloutPlan, index int, id string) error {
	start := plan.Now()
	last := "-"
	failed := 0

	for {
		read, err := rolloutRead(ctx, ops, index)
		if err == nil && id != "" && read.ID != "" && read.ID != id {
			return errRolloutReplaced
		}

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
