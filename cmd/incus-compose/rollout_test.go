package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	incusApi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/internal/testlib"
	"github.com/lxc/incus-compose/project"
	"github.com/lxc/incus-compose/shared"
)

// fakeReplica is a replica of the fake service: the statuses its healthcheck
// reports in turn, the last one for good.
type fakeReplica struct {
	fresh    bool
	running  bool
	drained  bool
	statuses []string
	seen     string
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

	// onSleep runs before each sleep, with how long it is for; afterSleep runs
	// once it is over.
	onSleep    func(d time.Duration)
	afterSleep func(d time.Duration)

	// onHealth runs after each read of a replica's health.
	onHealth func(status string)

	// fail makes the operation an event names fail; healthErrs fails that
	// many reads of a new replica's health before it answers.
	fail       map[string]error
	healthErrs int

	now    time.Time
	events []string

	// minReady is the fewest ready replicas the service may be left with.
	minReady int
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

	for _, r := range present {
		if len(r.statuses) > 0 {
			r.seen = r.statuses[0]
		}
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

func (f *fakeRollout) create(ctx context.Context, index int) error {
	err := f.op(ctx, fmt.Sprintf("create web-%d", index))
	if err != nil {
		return err
	}

	f.creates++

	if f.failCreate[f.creates] {
		f.record("create web-%d failed", index)
		return errors.New("clone failed")
	}

	f.replicas[index] = &fakeReplica{fresh: true, running: true, statuses: f.scripts[f.creates-1]}
	f.record("create web-%d", index)

	return nil
}

func (f *fakeRollout) health(ctx context.Context, index int) (string, bool, error) {
	err := ctx.Err()
	if err != nil {
		return "", false, err
	}

	r := f.replicas[index]
	if r.fresh && f.healthErrs > 0 {
		f.healthErrs--
		return "", false, errors.New("connection reset")
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

	return r.seen, r.running, nil
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
	f.record("%s", event)

	return nil
}

func (f *fakeRollout) retire(ctx context.Context, index int) error {
	err := f.op(ctx, fmt.Sprintf("retire web-%d", index))
	if err != nil {
		return err
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
		present = append(present, replica{index: i, fresh: r.fresh, running: r.running, drained: r.drained})
	}

	return rolloutPlan{
		Service:       "web",
		Replicas:      replicas,
		Present:       present,
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
	for i := 1; i <= 4; i++ {
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
		creates  int
		events   []string
	}{
		{
			name:     "one replica goes through the spare and comes back as web-1",
			replicas: 1,
			present:  map[int]*fakeReplica{1: old()},
			creates:  2,
			events: []string{
				"create web-2", "drain web-1", "retire web-1",
				"create web-1", "drain web-2", "retire web-2",
			},
		},
		{
			name:     "two replicas take one create more than they have",
			replicas: 2,
			present:  map[int]*fakeReplica{1: old(), 2: old()},
			creates:  3,
			events: []string{
				"create web-3", "drain web-1", "retire web-1",
				"create web-1", "drain web-2", "retire web-2",
				"create web-2", "drain web-3", "retire web-3",
			},
		},
		{
			name:     "a rollout cut short at the spare is picked up without a spare",
			replicas: 1,
			present:  map[int]*fakeReplica{2: old()},
			creates:  1,
			events:   []string{"create web-1", "drain web-2", "retire web-2"},
		},
		{
			name:     "a replica a killed rollout left drained is put back first",
			replicas: 1,
			present:  map[int]*fakeReplica{1: {running: true, drained: true, statuses: []string{shared.HealthStatusHealthy}}, 2: old()},
			creates:  1,
			events:   []string{"undrain web-1", "drain web-1", "retire web-1", "create web-1", "drain web-2", "retire web-2"},
		},
		{
			name:     "a stopped old replica goes first and is not drained",
			replicas: 2,
			present:  map[int]*fakeReplica{1: old(), 2: {statuses: []string{shared.HealthStatusStopped}}},
			creates:  3,
			events: []string{
				"create web-3", "retire web-2",
				"create web-2", "drain web-1", "retire web-1",
				"create web-1", "drain web-3", "retire web-3",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			scripts := make([][]string, tt.creates)
			for i := range scripts {
				scripts[i] = slowStart
			}

			f := newFakeRollout(t, tt.present, scripts...)
			plan := f.plan(tt.replicas)

			err := rollout(t.Context(), f, plan)
			require.NoError(t, err)

			assert.Equal(t, tt.events, f.events)
			assert.Equal(t, tt.creates, f.creates)

			want := []string{}
			for i := 1; i <= tt.replicas; i++ {
				want = append(want, fmt.Sprintf("web-%d new", i))
			}

			assert.Equal(t, want, f.final())
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

	// A new run sees what is left as old.
	for _, r := range f.replicas {
		r.fresh = false
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

// An interrupt that lands between two steps leaves both replicas serving, and
// one that lands as the old replica is stopped lets the stop finish.
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

		err := rollout(t.Context(), f, f.plan(1))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "connection reset")
		assert.Equal(t, []string{"create web-2", "discard web-2"}, f.events)
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
		"create web-1", "drain web-2", "retire web-2",
	}, f.events)
	assert.Equal(t, []string{"web-1 new"}, f.final())
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
			want:    []string{"its DNS aliases are answered by replica 1 alone and would go unanswered while it is replaced"},
		},
		{
			name:    "aliases given as a label",
			service: "image: docker.io/library/busybox\n    labels:\n      dns.aliases: cache",
			want:    []string{"its DNS aliases are answered by replica 1 alone and would go unanswered while it is replaced"},
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
