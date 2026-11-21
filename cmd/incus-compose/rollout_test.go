package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	incusApi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/client"
	"github.com/lxc/incus-compose/internal/testlib"
	"github.com/lxc/incus-compose/project"
	"github.com/lxc/incus-compose/shared"
)

// fakeReplica is a replica of the fake service: the statuses its healthcheck
// reports in turn, the last one for good. fresh is one a create of the test
// made, current one made from the service as it is now, as every create makes
// it: a run after the service changed sees none current.
//
// runID, drainBy and preStart are the marks of a rollout run: the run that
// made it, that drained it ("" for a drain by hand) and whose service-wide
// pre_start hooks it ran. A fixture that is fresh is one an earlier run made,
// marked "prev" unless it says otherwise. born orders the replicas by when
// they were made.
type fakeReplica struct {
	fresh    bool
	current  bool
	running  bool
	drained  bool
	statuses []string
	seen     string

	runID    string
	drainBy  string
	preStart string
	born     int64
}

// fakeRollout stands in for a service's replicas. It is not a fake Incus: it
// holds what the steps did to the replicas, so a test can ask whether the
// service was ever left with fewer ready replicas than it started with.
type fakeRollout struct {
	t        *testing.T
	replicas map[int]*fakeReplica

	// scripts are the statuses of each create in turn; failCreate fails one.
	scripts    [][]string
	failCreate map[int]bool
	creates    int

	// firsts are the indexes created as the rollout's first.
	firsts []int

	// onSleep runs before each sleep, with how long it is for; afterSleep runs
	// once it is over.
	onSleep    func(d time.Duration)
	afterSleep func(d time.Duration)

	// onHealth runs after each read of a replica's health, onRetire as a
	// retire is sent.
	onHealth func(status string)
	onRetire func()

	// retireErrs are returned by the retires in turn, before any acts.
	retireErrs []error

	// fail makes the operation an event names fail; healthErrs fails that
	// many reads of a new replica's health before it answers.
	fail       map[string]error
	healthErrs int

	now    time.Time
	events []string

	// minReady is the fewest ready replicas the service may be left with.
	minReady int

	// runID is the run the last plan made is for, runs how many plans were
	// made, and born the last birth handed out.
	runID string
	runs  int
	born  int64
}

func newFakeRollout(t *testing.T, present map[int]*fakeReplica, scripts ...[]string) *fakeRollout {
	f := &fakeRollout{
		t:          t,
		replicas:   present,
		scripts:    scripts,
		failCreate: map[int]bool{},
		fail:       map[string]error{},
		now:        time.Unix(0, 0),
	}

	for _, i := range slices.Sorted(maps.Keys(present)) {
		r := present[i]
		if len(r.statuses) > 0 {
			r.seen = r.statuses[0]
		}

		if r.fresh && r.runID == "" {
			r.runID = "prev"
		}

		f.born++
		r.born = f.born
	}

	f.minReady = f.ready()

	return f
}

// ready counts what ic-dns serves: running, not drained, last seen healthy.
func (f *fakeRollout) ready() int {
	n := 0
	for _, r := range f.replicas {
		if r.running && !r.drained && r.seen == shared.HealthStatusHealthy {
			n++
		}
	}

	return n
}

func (f *fakeRollout) record(format string, args ...any) {
	f.events = append(f.events, fmt.Sprintf(format, args...))

	assert.GreaterOrEqual(f.t, f.ready(), f.minReady, "after %q the service is left with fewer ready replicas", f.events[len(f.events)-1])
}

// op is the start of every operation but a health read: what a canceled
// context or an injected failure does to it.
func (f *fakeRollout) op(ctx context.Context, event string) error {
	err := ctx.Err()
	if err != nil {
		f.events = append(f.events, event+" canceled")
		return err
	}

	err = f.fail[event]
	if err != nil {
		f.events = append(f.events, event+" failed")
		return err
	}

	return nil
}

func (f *fakeRollout) create(ctx context.Context, index int, first bool) (string, error) {
	err := f.createReplica(ctx, index, first)
	if err != nil {
		return "", err
	}

	return fakeID(f.replicas[index]), nil
}

func (f *fakeRollout) createReplica(ctx context.Context, index int, first bool) error {
	err := f.op(ctx, fmt.Sprintf("create web-%d", index))
	if err != nil {
		return err
	}

	f.creates++

	if first {
		f.firsts = append(f.firsts, index)
	}

	if f.failCreate[f.creates] {
		f.record("create web-%d failed", index)
		return errors.New("clone failed")
	}

	f.born++

	r := &fakeReplica{fresh: true, current: true, running: true, statuses: f.scripts[f.creates-1], runID: f.runID, born: f.born}
	if first {
		r.preStart = f.runID
	}

	f.replicas[index] = r
	f.record("create web-%d", index)

	return nil
}

func (f *fakeRollout) health(ctx context.Context, index int) (replicaRead, error) {
	err := ctx.Err()
	if err != nil {
		return replicaRead{}, err
	}

	r := f.replicas[index]
	if r.fresh && f.healthErrs > 0 {
		f.healthErrs--
		return replicaRead{}, errors.New("connection reset")
	}

	if len(r.statuses) > 0 {
		r.seen = r.statuses[0]
		if len(r.statuses) > 1 {
			r.statuses = r.statuses[1:]
		}
	}

	if r.seen == "crashed" {
		r.running = false
	}

	if f.onHealth != nil {
		f.onHealth(r.seen)
	}

	return replicaRead{ID: fakeID(r), Status: r.seen, Running: r.running, Drained: r.drained, DrainedBy: r.drainBy}, nil
}

// fakeID tells a fake replica from another that took its index.
func fakeID(r *fakeReplica) string {
	return fmt.Sprintf("%p", r)
}

func (f *fakeRollout) unmark(_ context.Context, index int) error {
	r, ok := f.replicas[index]
	if ok {
		r.runID, r.preStart = "", ""
	}

	return nil
}

func (f *fakeRollout) drain(ctx context.Context, index int, drained bool) error {
	event := fmt.Sprintf("drain web-%d", index)
	if !drained {
		event = "un" + event
	}

	err := f.op(ctx, event)
	if err != nil {
		return err
	}

	f.replicas[index].drained = drained

	f.replicas[index].drainBy = ""
	if drained {
		f.replicas[index].drainBy = f.runID
	}

	f.record("%s", event)

	return nil
}

func (f *fakeRollout) retire(ctx context.Context, index int, stopped bool, _ time.Time) error {
	if f.onRetire != nil {
		f.onRetire()
	}

	// One that would begin past the round's freshness, or was started again.
	if len(f.retireErrs) > 0 {
		err := f.retireErrs[0]
		f.retireErrs = f.retireErrs[1:]
		f.events = append(f.events, fmt.Sprintf("retire web-%d put off", index))

		return err
	}

	err := f.op(ctx, fmt.Sprintf("retire web-%d", index))
	if err != nil {
		return err
	}

	if stopped && f.replicas[index].running {
		f.events = append(f.events, fmt.Sprintf("retire web-%d refused, it runs", index))
		return errors.New("started again")
	}

	delete(f.replicas, index)
	f.record("retire web-%d", index)

	return nil
}

func (f *fakeRollout) discard(ctx context.Context, index int, keep bool) error {
	err := f.op(ctx, fmt.Sprintf("discard web-%d", index))
	if err != nil {
		return err
	}

	r, ok := f.replicas[index]
	if !ok {
		f.record("discard web-%d (absent)", index)
		return nil
	}

	if keep {
		r.running = false
		f.record("stop web-%d", index)

		return nil
	}

	delete(f.replicas, index)
	f.record("discard web-%d", index)

	return nil
}

func (f *fakeRollout) sleep(ctx context.Context, d time.Duration) error {
	if f.onSleep != nil {
		f.onSleep(d)
	}

	err := ctx.Err()
	if err != nil {
		return err
	}

	f.now = f.now.Add(d)

	if f.afterSleep != nil {
		f.afterSleep(d)
	}

	return nil
}

func (f *fakeRollout) plan(replicas int) rolloutPlan {
	present := []replica{}
	for i, r := range f.replicas {
		present = append(present, replica{
			index: i, id: fakeID(r), created: time.Unix(r.born, 0),
			current: r.current, running: r.running, drained: r.drained,
			drainedBy: r.drainBy, runID: r.runID, preStart: r.preStart,
			healthy: r.seen == shared.HealthStatusHealthy,
		})
	}

	// Ascending, as rolloutPrepare reads them.
	slices.SortFunc(present, func(a, b replica) int { return a.index - b.index })

	// The run an interrupted one marked, picked up again, or a new one.
	f.runs++

	f.runID = rolloutAdoptedRun(present)
	if f.runID == "" {
		f.runID = fmt.Sprintf("run-%d", f.runs)
	}

	return rolloutPlan{
		Service:       "web",
		Replicas:      replicas,
		Present:       present,
		RunID:         f.runID,
		Name:          func(i int) string { return fmt.Sprintf("web-%d", i) },
		HealthTimeout: time.Minute,
		Grace:         35 * time.Second,
		Poll:          2 * time.Second,
		Now:           func() time.Time { return f.now },
		Out:           &bytes.Buffer{},
	}
}

func old() *fakeReplica {
	return &fakeReplica{running: true, statuses: []string{shared.HealthStatusHealthy}}
}

// final lists the replicas left, as "web-N new|old".
func (f *fakeRollout) final() []string {
	out := []string{}
	for i := 1; i <= 6; i++ {
		r, ok := f.replicas[i]
		if !ok {
			continue
		}

		what := "old"
		if r.fresh {
			what = "new"
		}

		if r.drained {
			what += " drained"
		}

		if !r.running {
			what += " stopped"
		}

		out = append(out, fmt.Sprintf("web-%d %s", i, what))
	}

	return out
}

var slowStart = []string{"", shared.HealthStatusStarting, shared.HealthStatusStarting, shared.HealthStatusHealthy}

func TestRolloutReplacesEveryReplica(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		replicas int
		present  map[int]*fakeReplica
		events   []string
		final    []string
	}{
		{
			name:     "one replica takes one create and moves to web-2",
			replicas: 1,
			present:  map[int]*fakeReplica{1: old()},
			events:   []string{"create web-2", "drain web-1", "retire web-1"},
			final:    []string{"web-2 new"},
		},
		{
			name:     "the next rollout of one replica moves it back to web-1",
			replicas: 1,
			present:  map[int]*fakeReplica{2: old()},
			events:   []string{"create web-1", "drain web-2", "retire web-2"},
			final:    []string{"web-1 new"},
		},
		{
			name:     "two replicas take two creates",
			replicas: 2,
			present:  map[int]*fakeReplica{1: old(), 2: old()},
			events: []string{
				"create web-3", "drain web-1", "retire web-1",
				"create web-1", "drain web-2", "retire web-2",
			},
			final: []string{"web-1 new", "web-3 new"},
		},
		{
			name:     "three replicas take three creates",
			replicas: 3,
			present:  map[int]*fakeReplica{1: old(), 2: old(), 3: old()},
			events: []string{
				"create web-4", "drain web-1", "retire web-1",
				"create web-1", "drain web-2", "retire web-2",
				"create web-2", "drain web-3", "retire web-3",
			},
			final: []string{"web-1 new", "web-2 new", "web-4 new"},
		},
		{
			name:     "two replicas on web-1 and web-3 fill the gap first",
			replicas: 2,
			present:  map[int]*fakeReplica{1: old(), 3: old()},
			events: []string{
				"create web-2", "drain web-1", "retire web-1",
				"create web-1", "drain web-3", "retire web-3",
			},
			final: []string{"web-1 new", "web-2 new"},
		},
		{
			name:     "fewer replicas than declared are added to before one is replaced",
			replicas: 2,
			present:  map[int]*fakeReplica{2: old()},
			events:   []string{"create web-1", "create web-3", "drain web-2", "retire web-2"},
			final:    []string{"web-1 new", "web-3 new"},
		},
		{
			name:     "a replica a killed rollout left drained, one more than declared, goes first and is not put back",
			replicas: 1,
			present:  map[int]*fakeReplica{1: {running: true, drained: true, drainBy: "prev", statuses: []string{shared.HealthStatusHealthy}}, 2: old()},
			events:   []string{"drain web-1", "retire web-1", "create web-1", "drain web-2", "retire web-2"},
			final:    []string{"web-1 new"},
		},
		{
			name:     "a replica a killed rollout left drained, within the declared count, is put back first",
			replicas: 2,
			present:  map[int]*fakeReplica{1: {running: true, drained: true, drainBy: "prev", statuses: []string{shared.HealthStatusHealthy}}, 2: old()},
			events: []string{
				"undrain web-1",
				"create web-3", "drain web-1", "retire web-1",
				"create web-1", "drain web-2", "retire web-2",
			},
			final: []string{"web-1 new", "web-3 new"},
		},
		{
			name:     "a drained replica is put back when the others do not serve the declared count",
			replicas: 1,
			present: map[int]*fakeReplica{
				1: {running: true, drained: true, drainBy: "prev", statuses: []string{shared.HealthStatusHealthy}},
				2: {running: true, statuses: []string{shared.HealthStatusUnhealthy}},
			},
			events: []string{"undrain web-1", "drain web-2", "retire web-2", "create web-2", "drain web-1", "retire web-1"},
			final:  []string{"web-2 new"},
		},
		{
			name:     "a stopped old replica goes first and is not drained",
			replicas: 2,
			present:  map[int]*fakeReplica{1: old(), 2: {statuses: []string{shared.HealthStatusStopped}}},
			events: []string{
				"create web-3", "retire web-2",
				"create web-2", "drain web-1", "retire web-1",
			},
			final: []string{"web-2 new", "web-3 new"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			scripts := make([][]string, tt.replicas)
			for i := range scripts {
				scripts[i] = slowStart
			}

			f := newFakeRollout(t, tt.present, scripts...)
			plan := f.plan(tt.replicas)

			err := rollout(t.Context(), f, plan)
			require.NoError(t, err)

			assert.Equal(t, tt.events, f.events)
			assert.Equal(t, tt.replicas, f.creates, "one create per replica")
			assert.Equal(t, tt.final, f.final())

			var firstCreate string
			for _, e := range tt.events {
				if strings.HasPrefix(e, "create ") {
					firstCreate = e
					break
				}
			}

			require.Len(t, f.firsts, 1, "only the first create runs the service-wide pre_start hooks")
			assert.Equal(t, firstCreate, fmt.Sprintf("create web-%d", f.firsts[0]))
		})
	}
}

// A new replica that does not make it is taken away, and the old ones were never
// drained: the service is exactly as it was.
func TestRolloutGivesUpOnANewReplica(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		script     []string
		failCreate bool
		keep       bool
		wantErr    error
		events     []string
		final      []string
	}{
		{
			name:    "unhealthy",
			script:  []string{shared.HealthStatusStarting, shared.HealthStatusUnhealthy},
			wantErr: errRolloutUnhealthy,
			events:  []string{"create web-2", "discard web-2"},
			final:   []string{"web-1 old"},
		},
		{
			name:    "starting for longer than the health timeout",
			script:  []string{shared.HealthStatusStarting},
			wantErr: errRolloutTimeout,
			events:  []string{"create web-2", "discard web-2"},
			final:   []string{"web-1 old"},
		},
		{
			name:    "stopped while starting",
			script:  []string{shared.HealthStatusStarting, "crashed"},
			wantErr: errRolloutStopped,
			events:  []string{"create web-2", "discard web-2"},
			final:   []string{"web-1 old"},
		},
		{
			name:       "the create fails",
			failCreate: true,
			events:     []string{"create web-2 failed", "discard web-2 (absent)"},
			final:      []string{"web-1 old"},
		},
		{
			name:    "with --keep-failed it stays, stopped",
			script:  []string{shared.HealthStatusUnhealthy},
			keep:    true,
			wantErr: errRolloutUnhealthy,
			events:  []string{"create web-2", "stop web-2"},
			final:   []string{"web-1 old", "web-2 new stopped"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, tt.script)
			f.failCreate[1] = tt.failCreate

			plan := f.plan(1)
			plan.KeepFailed = tt.keep

			err := rollout(t.Context(), f, plan)
			require.Error(t, err)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			}

			assert.Contains(t, err.Error(), "web-2")
			assert.Equal(t, tt.events, f.events)
			assert.Equal(t, tt.final, f.final())
		})
	}
}

// The second replica failing leaves the first one rolled out and the rest as
// they were, and a second run picks up from there.
func TestRolloutStopsAtTheFailingReplicaAndResumes(t *testing.T) {
	t.Parallel()

	f := newFakeRollout(t, map[int]*fakeReplica{1: old(), 2: old()},
		slowStart, []string{shared.HealthStatusUnhealthy})

	err := rollout(t.Context(), f, f.plan(2))
	require.ErrorIs(t, err, errRolloutUnhealthy)

	assert.Equal(t, []string{"create web-3", "drain web-1", "retire web-1", "create web-1", "discard web-1"}, f.events)
	assert.Equal(t, []string{"web-2 old", "web-3 new"}, f.final())

	// A new run keeps web-3, which is up to date, and replaces web-2 alone.
	f.events = nil
	f.scripts = append(f.scripts, slowStart)

	err = rollout(t.Context(), f, f.plan(2))
	require.NoError(t, err)

	assert.Equal(t, []string{"create web-1", "drain web-2", "retire web-2"}, f.events)
	assert.Equal(t, []string{"web-1 new", "web-3 new"}, f.final())
	assert.Equal(t, 3, f.creates)
}

// A new run after the service changed again finds nothing up to date and
// replaces every replica, the one an earlier run made included.
func TestRolloutResumesAfterTheServiceChanged(t *testing.T) {
	t.Parallel()

	f := newFakeRollout(t, map[int]*fakeReplica{1: old(), 2: old()},
		slowStart, []string{shared.HealthStatusUnhealthy})

	err := rollout(t.Context(), f, f.plan(2))
	require.ErrorIs(t, err, errRolloutUnhealthy)

	for _, r := range f.replicas {
		r.fresh = false
		r.current = false
	}

	f.events = nil
	f.scripts = append(f.scripts, slowStart, slowStart)

	err = rollout(t.Context(), f, f.plan(2))
	require.NoError(t, err)

	assert.Equal(t, []string{
		"create web-1", "drain web-2", "retire web-2",
		"create web-2", "drain web-3", "retire web-3",
	}, f.events)
	assert.Equal(t, []string{"web-1 new", "web-2 new"}, f.final())
}

// An interrupt never leaves a replica drained or a new one half there: before
// the new one is healthy it is taken away, during the grace the old one is put
// back and both stay.
func TestRolloutInterrupted(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		at     func(d time.Duration, grace time.Duration) bool
		events []string
		final  []string
	}{
		{
			name:   "while the new replica starts",
			at:     func(d, grace time.Duration) bool { return d != grace },
			events: []string{"create web-2", "discard web-2"},
			final:  []string{"web-1 old"},
		},
		{
			name:   "while the old replica drains",
			at:     func(d, grace time.Duration) bool { return d == grace },
			events: []string{"create web-2", "drain web-1", "undrain web-1"},
			final:  []string{"web-1 old", "web-2 new"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart, slowStart)
			plan := f.plan(1)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			f.onSleep = func(d time.Duration) {
				if tt.at(d, plan.Grace) {
					cancel()
				}
			}

			err := rollout(ctx, f, plan)
			require.ErrorIs(t, err, context.Canceled)
			assert.Contains(t, err.Error(), "interrupted")

			assert.Equal(t, tt.events, f.events)
			assert.Equal(t, tt.final, f.final())
		})
	}
}

// An interrupt that lands between two steps leaves both replicas serving, up
// to the stop of the old one: once the stop is sent it is let finish.
func TestRolloutInterruptedBetweenSteps(t *testing.T) {
	t.Parallel()

	t.Run("after the new replica is healthy, before the drain", func(t *testing.T) {
		t.Parallel()

		f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		f.onHealth = func(status string) {
			if status == shared.HealthStatusHealthy {
				cancel()
			}
		}

		err := rollout(ctx, f, f.plan(1))
		require.ErrorIs(t, err, context.Canceled)
		assert.Contains(t, err.Error(), "interrupted")

		assert.Equal(t, []string{"create web-2"}, f.events)
		assert.Equal(t, []string{"web-1 old", "web-2 new"}, f.final())
	})

	t.Run("at the end of the grace, as the stop begins", func(t *testing.T) {
		t.Parallel()

		f := newFakeRollout(t, map[int]*fakeReplica{1: old(), 2: old()}, slowStart)
		plan := f.plan(2)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		f.afterSleep = func(d time.Duration) {
			if d == plan.Grace {
				cancel()
			}
		}

		err := rollout(ctx, f, plan)
		require.ErrorIs(t, err, context.Canceled)

		// Before the stop is sent nothing is irreversible yet: the old one is
		// put back, and both serve.
		assert.Equal(t, []string{"create web-3", "drain web-1", "undrain web-1"}, f.events)
		assert.Equal(t, []string{"web-1 old", "web-2 old", "web-3 new"}, f.final())
	})

	t.Run("at the end of the grace of the last old replica", func(t *testing.T) {
		t.Parallel()

		f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
		plan := f.plan(1)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		f.afterSleep = func(d time.Duration) {
			if d == plan.Grace {
				cancel()
			}
		}

		err := rollout(ctx, f, plan)
		require.ErrorIs(t, err, context.Canceled)

		assert.Equal(t, []string{"create web-2", "drain web-1", "undrain web-1"}, f.events)
		assert.Equal(t, []string{"web-1 old", "web-2 new"}, f.final())
	})

	t.Run("once the stop has begun, which is seen through", func(t *testing.T) {
		t.Parallel()

		f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// The interrupt lands as the retire is sent.
		f.onRetire = cancel

		err := rollout(ctx, f, f.plan(1))
		require.NoError(t, err)

		assert.Equal(t, []string{"create web-2", "drain web-1", "retire web-1"}, f.events)
		assert.Equal(t, []string{"web-2 new"}, f.final())
	})
}

// A step that fails says so and leaves nothing drained that should serve.
func TestRolloutStepFails(t *testing.T) {
	t.Parallel()

	broken := errors.New("api down")

	for _, tt := range []struct {
		name   string
		fail   string
		events []string
		final  []string
		says   []string
	}{
		{
			name:   "the drain",
			fail:   "drain web-1",
			events: []string{"create web-2", "drain web-1 failed", "undrain web-1"},
			final:  []string{"web-1 old", "web-2 new"},
			says:   []string{"web-1: draining: api down"},
		},
		{
			name:   "the retire, after the new one serves",
			fail:   "retire web-1",
			events: []string{"create web-2", "drain web-1", "retire web-1 failed"},
			final:  []string{"web-1 old drained", "web-2 new"},
			says:   []string{"web-1: retiring: api down"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
			f.fail[tt.fail] = broken

			err := rollout(t.Context(), f, f.plan(1))
			require.ErrorIs(t, err, broken)

			for _, s := range tt.says {
				assert.Contains(t, err.Error(), s)
			}

			assert.Equal(t, tt.events, f.events)
			assert.Equal(t, tt.final, f.final())
		})
	}

	t.Run("taking the failed replica away", func(t *testing.T) {
		t.Parallel()

		f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, []string{shared.HealthStatusUnhealthy})
		f.fail["discard web-2"] = broken

		err := rollout(t.Context(), f, f.plan(1))
		require.ErrorIs(t, err, errRolloutUnhealthy)
		require.ErrorIs(t, err, broken)
		assert.Contains(t, err.Error(), "taking web-2 away")
	})

	t.Run("a few health reads in a row are tolerated", func(t *testing.T) {
		t.Parallel()

		f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart, slowStart)
		f.healthErrs = rolloutHealthReads - 1

		err := rollout(t.Context(), f, f.plan(1))
		require.NoError(t, err)
	})

	t.Run("more health reads in a row are not", func(t *testing.T) {
		t.Parallel()

		f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
		f.healthErrs = rolloutHealthReads

		// No verdict: the new one is not taken away, as it may be the one that
		// serves, and the run stops with an error.
		err := rollout(t.Context(), f, f.plan(1))
		require.ErrorIs(t, err, errRolloutUnread)
		assert.Contains(t, err.Error(), "connection reset")
		assert.Equal(t, []string{"create web-2"}, f.events)
		assert.Equal(t, []string{"web-1 old", "web-2 new"}, f.final())
	})
}

// With one replica too many, the unhealthy one is given back first, so the
// healthy one keeps serving throughout.
func TestRolloutSurplusGivesBackTheUnhealthyOne(t *testing.T) {
	t.Parallel()

	f := newFakeRollout(t, map[int]*fakeReplica{
		1: old(),
		2: {running: true, statuses: []string{shared.HealthStatusUnhealthy}},
	}, slowStart, slowStart)

	err := rollout(t.Context(), f, f.plan(1))
	require.NoError(t, err)

	assert.Equal(t, []string{
		"drain web-2", "retire web-2",
		"create web-2", "drain web-1", "retire web-1",
	}, f.events)
	assert.Equal(t, []string{"web-2 new"}, f.final())
}

// A rollout interrupted with one replica in flight is picked up again: one
// old replica goes before the next create, so there are never two more than
// declared, and it ends with the count declared.
func TestRolloutResumesAnInterruptedOne(t *testing.T) {
	t.Parallel()

	f := newFakeRollout(t, map[int]*fakeReplica{1: old(), 2: old()}, slowStart, slowStart, slowStart)
	plan := f.plan(2)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	f.onSleep = func(d time.Duration) {
		if d == plan.Grace {
			cancel()
		}
	}

	err := rollout(ctx, f, plan)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []string{"web-1 old", "web-2 old", "web-3 new"}, f.final())
	assert.Contains(t, plan.Out.(*bytes.Buffer).String(), "run `rollout web` again")

	f.events = nil
	f.onSleep = nil

	plan = f.plan(2)

	err = rollout(t.Context(), f, plan)
	require.NoError(t, err)

	// web-3 is up to date: an old one goes, and one create replaces the other.
	assert.Equal(t, []string{
		"drain web-1", "retire web-1",
		"create web-1", "drain web-2", "retire web-2",
	}, f.events)
	assert.Equal(t, []string{"web-1 new", "web-3 new"}, f.final())
	assert.Equal(t, 2, f.creates)
	assert.NotContains(t, plan.Out.(*bytes.Buffer).String(), "again")
}

// The case met for real: the retire of the old replica fails after the new
// one serves, which leaves the old one running drained next to it. Run again,
// the rollout keeps the new one and retires the old one, still drained: it
// neither puts the old one back nor retires the new one.
func TestRolloutResumesAfterAFailedRetire(t *testing.T) {
	t.Parallel()

	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	f.fail["retire web-1"] = errors.New("EOF")

	err := rollout(t.Context(), f, f.plan(1))
	require.Error(t, err)
	assert.Equal(t, []string{"web-1 old drained", "web-2 new"}, f.final())

	delete(f.fail, "retire web-1")

	f.events = nil
	plan := f.plan(1)

	var dry bytes.Buffer

	described := plan
	described.Out = &dry
	rolloutDescribe(described)
	assert.Contains(t, dry.String(), "now web-1 (running, drained), web-2 (running, up to date); 0 creates, ")

	err = rollout(t.Context(), f, plan)
	require.NoError(t, err)

	assert.Equal(t, []string{"drain web-1", "retire web-1"}, f.events)
	assert.Equal(t, []string{"web-2 new"}, f.final())
	assert.Equal(t, 1, f.creates, "no create on the second run")

	out := plan.Out.(*bytes.Buffer).String()
	assert.Contains(t, out, "web-1: drained by an earlier rollout, left so to be removed")
	assert.NotContains(t, out, "put back")
}

// What a rollout picked up again does with the replicas an earlier run left.
func TestRolloutResumes(t *testing.T) {
	t.Parallel()

	drainedOld := func() *fakeReplica {
		return &fakeReplica{running: true, drained: true, drainBy: "prev", statuses: []string{shared.HealthStatusHealthy}}
	}

	upToDate := func(statuses ...string) *fakeReplica {
		return &fakeReplica{fresh: true, current: true, running: true, statuses: statuses}
	}

	for _, tt := range []struct {
		name     string
		replicas int
		present  map[int]*fakeReplica
		scripts  [][]string
		wantErr  error
		events   []string
		final    []string
		creates  int
	}{
		{
			name:     "the new one, still starting, is waited for and the old one goes",
			replicas: 1,
			present:  map[int]*fakeReplica{1: upToDate(slowStart...), 2: drainedOld()},
			events:   []string{"drain web-2", "retire web-2"},
			final:    []string{"web-1 new"},
		},
		{
			name:     "the new one that turns unhealthy goes, and the old one is put back",
			replicas: 1,
			present:  map[int]*fakeReplica{1: upToDate(shared.HealthStatusStarting, shared.HealthStatusUnhealthy), 2: drainedOld()},
			wantErr:  errRolloutUnhealthy,
			events:   []string{"undrain web-2", "drain web-1", "retire web-1"},
			final:    []string{"web-2 old"},
		},
		{
			name:     "every replica up to date, one old one drained left over",
			replicas: 2,
			present:  map[int]*fakeReplica{1: upToDate(shared.HealthStatusHealthy), 2: upToDate(shared.HealthStatusHealthy), 3: drainedOld()},
			events:   []string{"drain web-3", "retire web-3"},
			final:    []string{"web-1 new", "web-2 new"},
		},
		{
			name:     "some up to date and some not: the old ones alone are replaced",
			replicas: 3,
			present:  map[int]*fakeReplica{1: upToDate(shared.HealthStatusHealthy), 2: old(), 3: old()},
			scripts:  [][]string{slowStart, slowStart},
			events: []string{
				"create web-4", "drain web-2", "retire web-2",
				"create web-2", "drain web-3", "retire web-3",
			},
			final:   []string{"web-1 new", "web-2 new", "web-4 new"},
			creates: 2,
		},
		{
			name:     "one more than declared, a drained one and an up to date one",
			replicas: 2,
			present:  map[int]*fakeReplica{1: drainedOld(), 2: old(), 3: upToDate(shared.HealthStatusHealthy)},
			scripts:  [][]string{slowStart},
			events: []string{
				"drain web-1", "retire web-1",
				"create web-1", "drain web-2", "retire web-2",
			},
			final:   []string{"web-1 new", "web-3 new"},
			creates: 1,
		},
		{
			name:     "all current, one replica and one drained: the drained one was being retired, and that is all left to do",
			replicas: 1,
			present: map[int]*fakeReplica{
				1: {current: true, running: true, statuses: []string{shared.HealthStatusHealthy}},
				2: {current: true, running: true, drained: true, drainBy: "prev", statuses: []string{shared.HealthStatusHealthy}},
			},
			events: []string{"drain web-2", "retire web-2"},
			final:  []string{"web-1 old"},
		},
		{
			name:     "all current with more replicas: the run's mark tells the new one, and the rest is replaced",
			replicas: 2,
			present: map[int]*fakeReplica{
				1: {current: true, running: true, statuses: []string{shared.HealthStatusHealthy}},
				2: {current: true, running: true, drained: true, drainBy: "prev", statuses: []string{shared.HealthStatusHealthy}},
				3: upToDate(shared.HealthStatusHealthy),
			},
			scripts: [][]string{slowStart},
			events: []string{
				"drain web-2", "retire web-2",
				"create web-2", "drain web-1", "retire web-1",
			},
			final:   []string{"web-2 new", "web-3 new"},
			creates: 1,
		},
		{
			name:     "no config hash, made by an older binary: old",
			replicas: 1,
			present:  map[int]*fakeReplica{1: drainedOld(), 2: old()},
			scripts:  [][]string{slowStart},
			events:   []string{"drain web-1", "retire web-1", "create web-1", "drain web-2", "retire web-2"},
			final:    []string{"web-1 new"},
			creates:  1,
		},
		{
			name:     "an up to date one stopped is old, and goes first",
			replicas: 1,
			present:  map[int]*fakeReplica{1: old(), 2: {current: true, statuses: []string{shared.HealthStatusStopped}}},
			scripts:  [][]string{slowStart},
			events:   []string{"retire web-2", "create web-2", "drain web-1", "retire web-1"},
			final:    []string{"web-2 new"},
			creates:  1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeRollout(t, tt.present, tt.scripts...)
			plan := f.plan(tt.replicas)

			// The plan says as many creates as the run takes, where it succeeds.
			var dry bytes.Buffer

			described := plan
			described.Out = &dry
			rolloutDescribe(described)

			if tt.wantErr == nil {
				assert.Regexp(t, fmt.Sprintf(`; %d creates?, `, tt.creates), dry.String())
			}

			err := rollout(t.Context(), f, plan)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.events, f.events)
			assert.Equal(t, tt.final, f.final())
			assert.Equal(t, tt.creates, f.creates)
		})
	}
}

// A rollout interrupted while it waits for a replica an earlier run left
// changes nothing.
func TestRolloutResumeInterrupted(t *testing.T) {
	t.Parallel()

	f := newFakeRollout(t, map[int]*fakeReplica{
		1: {fresh: true, current: true, running: true, statuses: []string{shared.HealthStatusStarting}},
		2: {running: true, drained: true, drainBy: "prev", statuses: []string{shared.HealthStatusHealthy}},
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	f.onSleep = func(time.Duration) { cancel() }

	err := rollout(ctx, f, f.plan(1))
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, f.events)
	assert.Equal(t, []string{"web-1 new", "web-2 old drained"}, f.final())
}

func TestRolloutHealthTimeout(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 3*time.Minute+rolloutHealthMargin, rolloutHealthTimeout(map[string]string{}),
		"docker's defaults: three retries of a 30s interval and a 30s timeout")

	assert.Equal(t, 2*time.Minute+5*8*time.Second+rolloutHealthMargin, rolloutHealthTimeout(map[string]string{
		shared.HealthKeyPrefix + "start_period": "2m",
		shared.HealthKeyPrefix + "interval":     "5s",
		shared.HealthKeyPrefix + "timeout":      "3s",
		shared.HealthKeyPrefix + "retries":      "5",
	}))
}

func TestRolloutDescribe(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer

	rolloutDescribe(rolloutPlan{
		Service:  "web",
		Replicas: 2,
		Present: []replica{
			{index: 1, running: true},
			{index: 2, running: true, drained: true, drainedBy: "r"},
			{index: 3},
		},
		Name:          func(index int) string { return fmt.Sprintf("web-%d", index) },
		HealthTimeout: 54 * time.Second,
		Grace:         35 * time.Second,
		Out:           &out,
	})

	assert.Equal(t, "web: can be rolled out, 2 replicas, now web-1 (running), web-2 (running, drained), web-3 (stopped); 2 creates, each new replica healthy within 54s, each old one kept 35s after its drain\n", out.String())

	out.Reset()

	// Picked up again: the up to date one takes no create.
	rolloutDescribe(rolloutPlan{
		Service:  "web",
		Replicas: 2,
		Present: []replica{
			{index: 1, running: true, drained: true, drainedBy: "r"},
			{index: 2, running: true},
			{index: 3, running: true, current: true, runID: "r"},
		},
		RunID:         "r",
		Name:          func(index int) string { return fmt.Sprintf("web-%d", index) },
		HealthTimeout: 54 * time.Second,
		Grace:         35 * time.Second,
		Out:           &out,
	})

	assert.Equal(t, "web: can be rolled out, 2 replicas, now web-1 (running, drained), web-2 (running), web-3 (running, up to date); 1 create, each new replica healthy within 54s, each old one kept 35s after its drain\n", out.String())

	out.Reset()

	// What a dry run left unchecked follows the plan.
	rolloutDescribe(rolloutPlan{
		Service:       "web",
		Replicas:      1,
		Present:       []replica{{index: 1, running: true}},
		Name:          func(index int) string { return fmt.Sprintf("web-%d", index) },
		HealthTimeout: 54 * time.Second,
		Grace:         35 * time.Second,
		Out:           &out,
		Notes:         []string{"its image is not in the project"},
	})

	assert.Equal(t, "web: can be rolled out, 1 replica, now web-1 (running); 1 create, each new replica healthy within 54s, each old one kept 35s after its drain\nweb: not checked: its image is not in the project\n", out.String())
}

func TestRolloutCurrent(t *testing.T) {
	t.Parallel()

	alias := &incusApi.ImageAliasesEntry{ImageAliasesEntryPut: incusApi.ImageAliasesEntryPut{Target: "new"}}
	hashed := func(hash, image string) map[string]string {
		return map[string]string{client.ConfigHashKey: hash, "volatile.base_image": image}
	}

	assert.True(t, rolloutCurrent(hashed("v1:a", "new"), "v1:a", alias))
	assert.False(t, rolloutCurrent(hashed("v1:a", "old"), "v1:a", nil), "no alias read: the image is not known to be the target")
	assert.False(t, rolloutCurrent(hashed("v1:a", "old"), "v1:a", &incusApi.ImageAliasesEntry{}), "an alias with no target")
	assert.False(t, rolloutCurrent(hashed("v1:a", "old"), "v1:a", alias), "another image")
	assert.False(t, rolloutCurrent(hashed("v1:b", "new"), "v1:a", alias), "another config")
	assert.False(t, rolloutCurrent(map[string]string{"volatile.base_image": "new"}, "v1:a", alias), "no hash: made by an older binary")
}

func TestRolloutStop(t *testing.T) {
	t.Parallel()

	errDeadline := errors.New("context deadline exceeded")
	errPID := errors.New("Invalid PID -1")

	// reads are the statuses the instance reads as, in turn, the last one for
	// good; a kill makes it read Stopped from the next read on, unless
	// restartAfterKill brings it back to Running once.
	type world struct {
		shutdownErr      error
		reads            []incusApi.StatusCode
		restartAfterKill bool
	}

	run := func(graceful bool, w world) (int, int, error) {
		var (
			shutdowns, kills, read int
			killed                 bool
		)

		shutdown := func(context.Context) error {
			shutdowns++

			return w.shutdownErr
		}

		kill := func(context.Context) error {
			kills++

			if w.restartAfterKill && kills == 1 {
				return errPID
			}

			killed = true

			return nil
		}

		status := func(context.Context) (incusApi.StatusCode, error) {
			if killed {
				return incusApi.Stopped, nil
			}

			s := w.reads[min(read, len(w.reads)-1)]
			read++

			if s == 0 {
				return 0, errPID
			}

			return s, nil
		}

		err := rolloutStop(t.Context(), shutdown, kill, status, graceful, 200*time.Millisecond, time.Millisecond, 20*time.Millisecond)

		return shutdowns, kills, err
	}

	t.Run("a graceful stop is all it takes", func(t *testing.T) {
		t.Parallel()

		shutdowns, kills, err := run(true, world{reads: []incusApi.StatusCode{incusApi.Stopped}})
		require.NoError(t, err)
		assert.Equal(t, 1, shutdowns)
		assert.Zero(t, kills)
	})

	t.Run("a graceful stop that ran out is followed by a kill", func(t *testing.T) {
		t.Parallel()

		_, kills, err := run(true, world{shutdownErr: errDeadline, reads: []incusApi.StatusCode{incusApi.Running}})
		require.NoError(t, err)
		assert.Equal(t, 1, kills)
	})

	t.Run("a stop Incus is still carrying out is waited for, not killed", func(t *testing.T) {
		t.Parallel()

		_, kills, err := run(true, world{shutdownErr: errDeadline, reads: []incusApi.StatusCode{0, incusApi.Error, incusApi.Stopping, incusApi.Stopped}})
		require.NoError(t, err)
		assert.Zero(t, kills)
	})

	t.Run("a stop that returned on a passing Error read is checked and killed", func(t *testing.T) {
		t.Parallel()

		// The client's Stop takes a moment in Error for "not running" and
		// returns nil without stopping anything.
		_, kills, err := run(true, world{reads: []incusApi.StatusCode{incusApi.Running}})
		require.NoError(t, err)
		assert.Equal(t, 1, kills)
	})

	t.Run("a kill that does not take is sent again", func(t *testing.T) {
		t.Parallel()

		_, kills, err := run(false, world{reads: []incusApi.StatusCode{incusApi.Running}, restartAfterKill: true})
		require.NoError(t, err)
		assert.Equal(t, 2, kills)
	})

	t.Run("without graceful it is killed straight away", func(t *testing.T) {
		t.Parallel()

		shutdowns, kills, err := run(false, world{reads: []incusApi.StatusCode{incusApi.Running}})
		require.NoError(t, err)
		assert.Zero(t, shutdowns)
		assert.Equal(t, 1, kills)
	})

	t.Run("one that never reads Stopped is an error with every cause", func(t *testing.T) {
		t.Parallel()

		_, _, err := run(true, world{shutdownErr: errDeadline, reads: []incusApi.StatusCode{incusApi.Error}})
		require.Error(t, err)
		require.ErrorIs(t, err, errDeadline)
		assert.Contains(t, err.Error(), "still Error")
	})
}

func TestRolloutTakeDown(t *testing.T) {
	t.Parallel()

	errRunning := errors.New("Instance is running")

	// restarts is how many times ic-healthd starts the replica again after a
	// stop; restarted it is running, and a delete then fails.
	run := func(keep bool, restarts int) (int, int, error) {
		var (
			stops, removes int
			running        bool
		)

		stop := func(context.Context) error {
			stops++
			running = false

			return nil
		}

		restart := func() {
			if restarts > 0 {
				restarts--
				running = true
			}
		}

		remove := func(context.Context) error {
			removes++

			restart()
			if running {
				return errRunning
			}

			return nil
		}

		isRunning := func(context.Context) (bool, error) { return running, nil }

		startsAgain := func(context.Context) (bool, error) {
			restart()

			return running, nil
		}

		err := rolloutTakeDown(t.Context(), keep, 3, stop, remove, isRunning, startsAgain)

		return stops, removes, err
	}

	t.Run("deleted at the first go", func(t *testing.T) {
		t.Parallel()

		stops, removes, err := run(false, 0)
		require.NoError(t, err)
		assert.Equal(t, 1, stops)
		assert.Equal(t, 1, removes)
	})

	t.Run("started again by a restart under way, stopped again and deleted", func(t *testing.T) {
		t.Parallel()

		stops, removes, err := run(false, 1)
		require.NoError(t, err)
		assert.Equal(t, 2, stops)
		assert.Equal(t, 2, removes)
	})

	t.Run("kept, it is watched and stopped again", func(t *testing.T) {
		t.Parallel()

		stops, removes, err := run(true, 1)
		require.NoError(t, err)
		assert.Equal(t, 2, stops)
		assert.Zero(t, removes)
	})

	t.Run("one that keeps coming back is given up on", func(t *testing.T) {
		t.Parallel()

		stops, _, err := run(false, 10)
		require.Error(t, err)
		assert.Equal(t, 3, stops)
	})
}

func TestRolloutBlockers(t *testing.T) {
	t.Parallel()

	checked := map[string]string{
		shared.HealthEnabledKey:         "true",
		shared.HealthKeyPrefix + "test": `["CMD","true"]`,
	}

	plain := func() *incusApi.Instance {
		return &incusApi.Instance{
			InstancePut: incusApi.InstancePut{Config: map[string]string{}},
			ExpandedDevices: map[string]map[string]string{
				"root": {"type": "disk", "path": "/", "pool": "remote"},
				"eth0": {"type": "nic", "network": "app"},
			},
		}
	}

	for _, tt := range []struct {
		name      string
		service   string
		health    map[string]string
		old       func(i *incusApi.Instance)
		declared  map[string]map[string]string
		pools     map[string]string
		clustered bool
		want      []string
	}{
		{
			name:      "a checked service on a remote root disk rolls out",
			service:   "image: docker.io/library/busybox",
			clustered: true,
		},
		{
			name:    "no healthcheck",
			service: "image: docker.io/library/busybox",
			health:  map[string]string{shared.HealthEnabledKey: "true", shared.HealthKeyPrefix + "test": `["NONE"]`},
			want:    []string{"it has no healthcheck, so no new replica can be waited for and ic-dns serves it before it is ready"},
		},
		{
			name:    "marked as a singleton",
			service: "image: docker.io/library/busybox\n    x-incus-compose:\n      rollout: false",
			want:    []string{"x-incus-compose.rollout: false marks it as one that must not run twice"},
		},
		{
			name:    "container_name",
			service: "image: docker.io/library/busybox\n    container_name: beat",
			want:    []string{`container_name "beat" is one name, which the new and the old replica cannot share`},
		},
		{
			name:    "published ports",
			service: "image: docker.io/library/busybox\n    ports: [\"8080:80\"]",
			want:    []string{"its published ports cannot be bound by the new and the old replica at once"},
		},
		{
			name:    "a fixed IP in the compose file and on the NIC count once",
			service: "image: docker.io/library/busybox\n    networks:\n      app:\n        ipv4_address: 10.1.2.3",
			old: func(i *incusApi.Instance) {
				i.ExpandedDevices["eth0"]["ipv4.address"] = "10.1.2.3"
			},
			want: []string{"a fixed IP or MAC address cannot be held by the new and the old replica at once"},
		},
		{
			name:    "a MAC address set through x-incus",
			service: "image: docker.io/library/busybox",
			old: func(i *incusApi.Instance) {
				i.ExpandedDevices["eth0"]["hwaddr"] = "00:16:3e:00:00:01"
			},
			want: []string{"a fixed IP or MAC address cannot be held by the new and the old replica at once"},
		},
		{
			name:    "network aliases",
			service: "image: docker.io/library/busybox\n    networks:\n      app:\n        aliases: [cache]",
			want:    []string{"its DNS aliases are answered by the first replica alone and would go unanswered while it is replaced"},
		},
		{
			name:    "aliases given as a label",
			service: "image: docker.io/library/busybox\n    labels:\n      dns.aliases: cache",
			want:    []string{"its DNS aliases are answered by the first replica alone and would go unanswered while it is replaced"},
		},
		{
			name:    "a proxy device from a profile",
			service: "image: docker.io/library/busybox",
			old: func(i *incusApi.Instance) {
				i.ExpandedDevices["metrics"] = map[string]string{"type": "proxy", "listen": "tcp:10.0.4.1:3100"}
			},
			want: []string{`its proxy device "metrics" binds a host address the new and the old replica cannot both hold`},
		},
		{
			name:     "a proxy device only the compose file has so far",
			service:  "image: docker.io/library/busybox",
			declared: map[string]map[string]string{"admin": {"type": "proxy", "listen": "tcp:0.0.0.0:9000"}},
			want:     []string{`its proxy device "admin" binds a host address the new and the old replica cannot both hold`},
		},
		{
			name:    "a dns.drain label: no new replica would serve",
			service: "image: docker.io/library/busybox\n    labels:\n      dns.drain: \"true\"",
			want:    []string{rolloutDrainedBlocker},
		},
		{
			name:    "a rollout key that is not a boolean refuses",
			service: "image: docker.io/library/busybox\n    x-incus-compose:\n      rollout: [no]",
			want:    []string{"x-incus-compose.rollout must be true or false"},
		},
		{
			name:    "a volume on a block pool of a cluster",
			service: "image: docker.io/library/busybox",
			old: func(i *incusApi.Instance) {
				i.ExpandedDevices["data"] = map[string]string{"type": "disk", "path": "/data", "pool": "remote", "source": "web-data"}
			},
			pools:     map[string]string{"remote": "linstor"},
			clustered: true,
			want:      []string{`volume "web-data" on pool "remote" (linstor) is attached on one cluster member at a time, and the new replica may land on another`},
		},
		{
			name:    "a volume on cephfs is shared across members",
			service: "image: docker.io/library/busybox",
			old: func(i *incusApi.Instance) {
				i.ExpandedDevices["data"] = map[string]string{"type": "disk", "path": "/data", "pool": "shared", "source": "web-data"}
			},
			pools:     map[string]string{"shared": "cephfs"},
			clustered: true,
		},
		{
			name:    "a volume on a standalone server is mounted from the one host",
			service: "image: docker.io/library/busybox",
			old: func(i *incusApi.Instance) {
				i.ExpandedDevices["data"] = map[string]string{"type": "disk", "path": "/data", "pool": "default", "source": "web-data"}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := testlib.WriteTempFiles(t, map[string]string{
				"compose.yaml": "name: blockers\nservices:\n  web:\n    " + tt.service + "\nnetworks:\n  app: {}\n",
			})

			p, err := project.New().Load(t.Context(), project.LoadWorkingDir(dir))
			require.NoError(t, err)

			health := tt.health
			if health == nil {
				health = checked
			}

			inst := plain()
			if tt.old != nil {
				tt.old(inst)
			}

			declared := tt.declared
			if declared == nil {
				declared = map[string]map[string]string{}
			}

			got := rolloutBlockers(p.Services["web"], health, inst.Config, []map[string]map[string]string{inst.ExpandedDevices, declared}, tt.pools, tt.clustered)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRolloutWatched(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		health map[string]string
		want   bool
	}{
		{name: "nothing", want: false},
		{name: "a test without enabled", health: map[string]string{shared.HealthKeyPrefix + "test": `["CMD","true"]`}, want: false},
		{name: "a restart policy alone", health: map[string]string{shared.HealthEnabledKey: "true"}, want: false},
		{name: "a test", health: map[string]string{shared.HealthEnabledKey: "true", shared.HealthKeyPrefix + "test": `["CMD-SHELL","true"]`}, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, rolloutWatched(tt.health))
		})
	}
}

// TestE2ERolloutLeavesDependentsAlone rolls out a service another one depends
// on: the service gets a new web-1, the dependent is the instance it was.
func TestE2ERolloutLeavesDependentsAlone(t *testing.T) {
	t.Parallel()
	testlib.SkipE2E(t)

	compose := testlib.Fixture(t, "proxy", "compose.yaml")

	ctx := t.Context()
	pn := t.Name()

	testlib.CleanupCompose(t, pn, "-f", compose, "down", "--project")

	_, err := testlib.RunCompose(ctx, t, pn, "", nil, "-f", compose, "up", "--detach")
	require.NoError(t, err)

	c := projectClient(ctx, t, pn)
	conn, err := c.Connection()
	require.NoError(t, err)

	uuid := func(name string) string {
		inst, _, err := conn.GetInstance(ctx, c.IncusProject(), name, nil)
		require.NoError(t, err)

		return inst.Config["volatile.uuid"]
	}

	backend, frontend := uuid("backend1-1"), uuid("frontend-1")

	_, err = testlib.RunCompose(ctx, t, pn, "", nil, "-f", compose, "rollout", "--dns-ttl", "0s", "--drain-delay", "1s", "backend1")
	require.NoError(t, err)

	assert.NotEqual(t, backend, uuid("backend1-1"), "backend1-1 is a new instance")
	assert.Equal(t, frontend, uuid("frontend-1"), "the dependent is left alone")

	absent, err := instanceAbsent(ctx, c, "backend1-2")
	require.NoError(t, err)
	assert.True(t, absent, "the spare is gone")
}

// TestE2ERolloutNeverHealthy gives the new replica a check that never passes:
// the rollout fails, the spare is gone and the old replica is the one it was.
func TestE2ERolloutNeverHealthy(t *testing.T) {
	t.Parallel()
	testlib.SkipE2E(t)

	compose := testlib.Fixture(t, "proxy", "compose.yaml")
	broken := testlib.WriteTempFiles(t, map[string]string{
		"compose.broken.yaml": "services:\n  backend1:\n    healthcheck:\n      test: [\"CMD\", \"false\"]\n      interval: 2s\n      retries: 1\n",
	})

	ctx := t.Context()
	pn := t.Name()

	testlib.CleanupCompose(t, pn, "-f", compose, "down", "--project")

	_, err := testlib.RunCompose(ctx, t, pn, "", nil, "-f", compose, "up", "--detach")
	require.NoError(t, err)

	c := projectClient(ctx, t, pn)
	conn, err := c.Connection()
	require.NoError(t, err)

	before, _, err := conn.GetInstance(ctx, c.IncusProject(), "backend1-1", nil)
	require.NoError(t, err)

	_, err = testlib.RunCompose(ctx, t, pn, "", nil, "-f", compose, "-f", broken+"/compose.broken.yaml", "rollout", "--dns-ttl", "0s", "--drain-delay", "1s", "backend1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backend1-2")

	after, _, err := conn.GetInstance(ctx, c.IncusProject(), "backend1-1", nil)
	require.NoError(t, err)
	assert.Equal(t, before.Config["volatile.uuid"], after.Config["volatile.uuid"])
	assert.Equal(t, incusApi.Running, after.StatusCode)
	assert.NotEqual(t, "true", after.Config[shared.DNSDrainKey], "the old replica is not left drained")

	absent, err := instanceAbsent(ctx, c, "backend1-2")
	require.NoError(t, err)
	assert.True(t, absent, "the failed replica is gone")
}
