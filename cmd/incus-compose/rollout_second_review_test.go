package main

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/shared"
)

type secondReviewReads struct {
	*fakeRollout
	beforeRead func(context.Context, int) error
}

func (f secondReviewReads) health(ctx context.Context, index int) (replicaRead, error) {
	err := f.beforeRead(ctx, index)
	if err != nil {
		return replicaRead{}, err
	}
	return f.fakeRollout.health(ctx, index)
}

func TestSecondReviewRollbackDuringGrace(t *testing.T) {
	for _, resumed := range []bool{false, true} {
		name := "ordinary"
		if resumed {
			name = "resumed"
		}
		t.Run(name, func(t *testing.T) {
			rs := map[int]*fakeReplica{1: old()}
			if resumed {
				// Drained by the earlier rollout, which marks its drains: an
				// unmarked drain is one by hand, which no rollout takes off.
				rs[1].drained = true
				rs[1].drainBy = "prev"
				rs[2] = reviewCurrent()
			}
			f := newFakeRollout(t, rs, slowStart)
			f.minReady = 0
			plan := f.plan(1)
			f.onSleep = func(d time.Duration) {
				if d == plan.Grace {
					rs[2].statuses = []string{shared.HealthStatusUnhealthy}
					rs[2].seen = shared.HealthStatusUnhealthy
				}
			}
			err := rollout(t.Context(), f, plan)
			require.Error(t, err)
			require.Equal(t, []string{"web-1 old"}, f.final(), "err=%v events=%v", err, f.events)
		})
	}
}

func TestSecondReviewExternalDrain(t *testing.T) {
	r := old()
	r.drained = true // The operator put it in maintenance before any rollout.
	f := newFakeRollout(t, map[int]*fakeReplica{1: r}, slowStart)
	err := rollout(t.Context(), f, f.plan(1))
	require.NotContains(t, f.events, "undrain web-1", "err=%v", err)
}

func TestSecondReviewUnchangedResume(t *testing.T) {
	oldTwo := old()
	oldTwo.current = true
	oldTwo.drained = true
	oldThree := old()
	oldThree.current = true
	// The interrupted run started from indexes 2 and 3 and created index 1.
	// Its marks say so: web-1 is its own and ran its pre_start hooks, and it
	// drained web-2 (adapted: the marks are what tells new from old now).
	oldTwo.drainBy = "prev"
	newOne := reviewCurrent()
	newOne.preStart = "prev"
	f := newFakeRollout(t, map[int]*fakeReplica{1: newOne, 2: oldTwo, 3: oldThree}, slowStart, slowStart)
	err := rollout(t.Context(), f, f.plan(2))
	require.NoError(t, err)
	if f.replicas[1] != newOne || len(f.firsts) > 0 {
		t.Fatalf("new replica kept=%t, repeated hook=%v, events=%v", f.replicas[1] == newOne, f.firsts, f.events)
	}
}

func TestSecondReviewStoppedAfterHook(t *testing.T) {
	r := reviewCurrent()
	r.running = false // The hook completed before Start sent the start operation.
	// A hook that succeeded records it on the replica before the start
	// (adapted: the mark is what the scenario leaves behind).
	r.preStart = "prev"
	f := newFakeRollout(t, map[int]*fakeReplica{1: old(), 2: r}, slowStart)
	err := rollout(t.Context(), f, f.plan(1))
	require.NoError(t, err)
	require.Empty(t, f.firsts, "events=%v", f.events)
}

func TestSecondReviewSingleEOF(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	failed := false
	ops := secondReviewReads{f, func(_ context.Context, i int) error {
		if i == 1 && !failed {
			failed = true
			return io.EOF
		}
		return nil
	}}
	err := rollout(t.Context(), ops, f.plan(1))
	require.NoError(t, err, "events=%v", f.events)
	require.True(t, failed)
	require.Equal(t, []string{"web-2 new"}, f.final())
}

func TestSecondReviewSlowSweep(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old(), 2: old()}, slowStart, slowStart)
	f.minReady = 0
	plan := f.plan(2)
	afterGrace := false
	f.afterSleep = func(d time.Duration) {
		afterGrace = d == plan.Grace
	}
	ops := secondReviewReads{f, func(_ context.Context, i int) error {
		if afterGrace && i == 3 {
			// The previous GET found web-2 healthy. During this GET it failed.
			f.now = f.now.Add(time.Minute)
			f.replicas[2].statuses = []string{shared.HealthStatusUnhealthy}
			f.replicas[2].seen = shared.HealthStatusUnhealthy
			afterGrace = false
		}
		return nil
	}}
	err := rollout(t.Context(), ops, plan)
	require.NotContains(t, f.events, "retire web-1", "err=%v, events=%v", err, f.events)
}

func TestSecondReviewStoppedVictimBudget(t *testing.T) {
	stopped := old()
	stopped.running = false
	unhealthy := old()
	unhealthy.statuses = []string{shared.HealthStatusUnhealthy}
	f := newFakeRollout(t, map[int]*fakeReplica{1: stopped, 2: unhealthy}, slowStart, slowStart)
	err := rollout(t.Context(), f, f.plan(2))

	// Contested: the review asked for the stopped web-1 to be kept while only
	// web-3 serves. A stopped replica serves nothing, so retiring it never
	// lowers the count that serves; holding it back would stop a degraded
	// service from being rolled out of its state. It goes, and the rollout
	// goes on to leave two healthy replicas. The other half of the point, a
	// victim that starts again between its read and its removal, is covered:
	// retire refuses one that runs (TestSecondReviewStoppedVictimRestarts).
	require.NoError(t, err, "events=%v", f.events)
	require.Equal(t, 2, f.ready(), "events=%v", f.events)
}

func TestSecondReviewStoppedVictimRestarts(t *testing.T) {
	stopped := old()
	stopped.running = false
	f := newFakeRollout(t, map[int]*fakeReplica{1: stopped, 2: old()}, slowStart)
	plan := f.plan(1)

	// ic-healthd starts web-1 again after the rollout's last read of it, the
	// round right before its removal (two rounds of two reads).
	reads := 0
	f.onHealth = func(string) {
		reads++
		if reads == 4 {
			f.replicas[1].running = true
		}
	}

	err := rollout(t.Context(), f, plan)
	require.Error(t, err)
	require.Contains(t, f.events, "retire web-1 refused, it runs", "events=%v", f.events)
	require.NotNil(t, f.replicas[1])
}

func TestSecondReviewUnreadOnlyServer(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	f.minReady = 0
	ops := secondReviewReads{f, func(_ context.Context, i int) error {
		if i == 2 {
			f.replicas[1].running = false
			f.replicas[2].seen = shared.HealthStatusHealthy
			return io.EOF
		}
		return nil
	}}
	err := rollout(t.Context(), ops, f.plan(1))
	require.ErrorIs(t, err, errRolloutUnread)
	require.Equal(t, 1, f.ready(), "events=%v", f.events)
}

func TestSecondReviewStalledStop(t *testing.T) {
	// The kill returns immediately if it would otherwise wait without a deadline.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	settle := 5 * time.Millisecond
	hadDeadline := false
	kill := func(callCtx context.Context) error {
		_, hadDeadline = callCtx.Deadline()
		if !hadDeadline {
			return errors.New("request would wait for the server")
		}
		<-callCtx.Done()
		return callCtx.Err()
	}
	status := func(context.Context) (api.StatusCode, error) { return api.Running, nil }
	_ = rolloutStop(ctx, nil, kill, status, false, settle, time.Millisecond, time.Second)
	require.True(t, hadDeadline, "the in-flight kill has no settle deadline")
}

func TestSecondReviewLateHealthy(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, []string{shared.HealthStatusHealthy})
	plan := f.plan(1)
	plan.HealthTimeout = time.Second
	late := false
	ops := secondReviewReads{f, func(_ context.Context, i int) error {
		if i == 2 && !late {
			f.now = f.now.Add(time.Hour)
			late = true
		}
		return nil
	}}
	err := rollout(t.Context(), ops, plan)
	require.Error(t, err, "healthy after one hour, budget one second, events=%v", f.events)
	require.False(t, slices.Contains(f.events, "retire web-1"))
}

func TestSecondReviewCancelDuringFinalRead(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	plan := f.plan(1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	afterGrace := false
	f.afterSleep = func(d time.Duration) { afterGrace = d == plan.Grace }
	ops := secondReviewReads{f, func(callCtx context.Context, _ int) error {
		if afterGrace {
			cancel()
			return callCtx.Err()
		}
		return nil
	}}
	err := rollout(ctx, ops, plan)
	require.ErrorIs(t, err, context.Canceled, "events=%v", f.events)
	require.NotContains(t, f.events, "retire web-1")
}

func TestSecondReviewIdentityAtStop(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	f.minReady = 0
	plan := f.plan(1)
	original := f.replicas[1]
	foreign := old()
	f.afterSleep = func(d time.Duration) {
		if d == plan.Grace && f.replicas[1] == original {
			// During the grace someone removed the drained victim and reused
			// the Incus name for an independent, undrained instance.
			f.replicas[1] = foreign
		}
	}
	err := rollout(t.Context(), f, plan)
	require.Same(t, foreign, f.replicas[1], "err=%v, events=%v", err, f.events)
}

func TestSecondReviewDrainClearedDuringGrace(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	plan := f.plan(1)
	f.afterSleep = func(d time.Duration) {
		if d == plan.Grace {
			f.replicas[1].drained = false
			f.record("operator undrain web-1")
			f.afterSleep = nil
		}
	}
	err := rollout(t.Context(), f, plan)
	moved := slices.Index(f.events, "operator undrain web-1")
	require.NotEqual(t, -1, moved)
	after := f.events[moved+1:]
	retired := slices.Index(after, "retire web-1")
	if retired >= 0 {
		require.Contains(t, after[:retired], "drain web-1", "err=%v events=%v", err, f.events)
	}
}

func TestSecondReviewMixedFleetFirstRollout(t *testing.T) {
	// An interrupted up recreated one replica stop-first. No rollout has
	// started or run its service hook in this update.
	fromUp := old()
	fromUp.current = true
	// Two scripts: with no mark, the replica up made is old too, and both are
	// replaced (adapted: the review's single script assumed one create).
	f := newFakeRollout(t, map[int]*fakeReplica{1: old(), 2: fromUp}, slowStart, slowStart)
	err := rollout(t.Context(), f, f.plan(2))
	require.NoError(t, err)
	require.Len(t, f.firsts, 1, "events=%v", f.events)
}
