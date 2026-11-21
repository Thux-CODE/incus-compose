package main

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/compose-spec/compose-go/v2/types"
	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/client"
	"github.com/lxc/incus-compose/project"
	"github.com/lxc/incus-compose/shared"
)

func TestFourthReviewProfileRemoved(t *testing.T) {
	svc := types.ServiceConfig{
		Name: "web", Image: "images:alpine/edge", Restart: "always",
		Extensions: types.Extensions{"x-incus-compose": map[string]any{"profiles": []any{"default"}}},
	}
	p := project.New()
	p.Project = &types.Project{Name: "shop", Services: types.Services{"web": svc}}
	c := client.NewOfflineClient(t.Context(), "shop")
	resources, err := p.Resources(c)
	require.NoError(t, err)

	var replacement *client.Instance
	for _, res := range resources["web"] {
		inst, ok := res.(*client.Instance)
		if ok {
			replacement = inst
			break
		}
	}

	require.NotNil(t, replacement)
	require.Equal(t, []string{"default"}, replacement.Config.Profiles)
	local := replacement.Config.Extensions
	require.Equal(t, "always", local[shared.HealthKeyPrefix+"restart"])
	require.Equal(t, "true", local[shared.HealthEnabledKey])
	require.False(t, rolloutWatched(local))

	old := &api.Instance{
		InstancePut: api.InstancePut{Profiles: []string{"default", "probe"}, Config: local},
		ExpandedConfig: map[string]string{
			shared.HealthEnabledKey: "true", shared.HealthKeyPrefix + "test": `["CMD","/probe"]`,
		},
	}
	// Adapted: the healthcheck is taken from the profiles the compose file
	// declares now, read from Incus (here default, with no check), and no
	// longer from the old replica.
	_ = old
	got := rolloutProfileHealth(local, []map[string]string{{}})
	require.False(t, rolloutWatched(got), "replacement has only default, which has no probe: %v", got[shared.HealthKeyPrefix+"test"])
}

func TestFourthReviewProfileShadowed(t *testing.T) {
	old := &api.Instance{
		InstancePut: api.InstancePut{
			Profiles: []string{"probe"},
			Config: map[string]string{
				shared.HealthEnabledKey: "true", shared.HealthKeyPrefix + "test": `["CMD","/old"]`,
			},
		},
	}
	old.ExpandedConfig = old.Config
	// Both instances use probe. Its check is shadowed only on the old instance.
	// Adapted: the probe profile, read as it is, gives the check; the old
	// replica's own keys do not count.
	_ = old
	probe := map[string]string{shared.HealthEnabledKey: "true", shared.HealthKeyPrefix + "test": `["CMD","/probe"]`}
	got := rolloutProfileHealth(nil, []map[string]string{probe})
	require.True(t, rolloutWatched(got), "replacement inherits enabled=true and test from probe")
}

func TestFourthReviewStoppedRollback(t *testing.T) {
	r := old()
	r.running = false
	r.drained, r.drainBy = true, "interrupted"
	f := newFakeRollout(t, map[int]*fakeReplica{1: r},
		[]string{shared.HealthStatusHealthy, shared.HealthStatusHealthy, shared.HealthStatusUnhealthy})
	err := rollout(t.Context(), f, f.plan(1))
	require.Error(t, err)
	require.Same(t, r, f.replicas[1])

	// The pending restart completes, with the old replica healthy again.
	r.running = true
	require.Positive(t, f.ready(), "events=%v; left %v", f.events, f.final())
}

func TestFourthReviewStoppedRetry(t *testing.T) {
	r := old()
	r.running = false
	f := newFakeRollout(t, map[int]*fakeReplica{1: r},
		[]string{shared.HealthStatusHealthy, shared.HealthStatusHealthy, shared.HealthStatusUnhealthy},
		[]string{shared.HealthStatusHealthy})
	err := rollout(t.Context(), f, f.plan(1))
	require.Error(t, err)
	err = rollout(t.Context(), f, f.plan(1))
	require.NoError(t, err, "a fresh create would serve; events=%v", f.events)
}

func TestFourthReviewLastStaleStop(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	f.minReady = 0
	f.retireErrs = []error{errRolloutStale, errRolloutStale, errRolloutStale}
	f.onRetire = func() {
		if len(f.retireErrs) == 1 {
			f.replicas[2].seen = shared.HealthStatusUnhealthy
			f.replicas[2].statuses = []string{shared.HealthStatusUnhealthy}
		}
	}
	err := rollout(t.Context(), f, f.plan(1))
	require.ErrorIs(t, err, errRolloutStale)
	require.Positive(t, f.ready(), "no stop was issued; events=%v; left %v", f.events, f.final())
}

func TestFourthReviewFailedUnmarkOnStaleStop(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	f.retireErrs = []error{errors.Join(errRolloutStale, io.ErrUnexpectedEOF)}
	// The application fails before the second round. Its old replica must
	// resume with a working restart policy, or the rollback must report why.
	f.onRetire = func() {
		f.replicas[2].statuses = []string{shared.HealthStatusUnhealthy}
	}
	f.minReady = 0
	err := rollout(t.Context(), f, f.plan(1))
	require.ErrorIs(t, err, io.ErrUnexpectedEOF, "failure to restore restart policy was lost: %v", err)
}

func TestFourthReviewRestartBeforeStatus(t *testing.T) {
	running := true
	kills := 0
	shutdown := func(context.Context) error {
		running = false
		// An already pending healthd restart completes before the next GET.
		running = true
		return nil
	}
	status := func(context.Context) (api.StatusCode, error) {
		if running {
			return api.Running, nil
		}
		return api.Stopped, nil
	}
	kill := func(context.Context) error {
		kills++
		running = false
		return nil
	}
	err := rolloutStop(t.Context(), shutdown, kill, status, true, time.Second, time.Millisecond, time.Millisecond)
	require.Zero(t, kills, "successful shutdown followed by running must request a fresh fleet round; err=%v", err)
}

func TestFourthReviewCancelBeforeStop(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	f.onRetire = cancel
	err := rollout(ctx, f, f.plan(1))
	require.ErrorIs(t, err, context.Canceled, "cancel before the mutating stop; events=%v", f.events)
}

type fourthReviewStopDeadline struct {
	*fakeRollout
	notAfter time.Time
}

// Adapted: retire also takes killAfterStop since a later change.
func (f *fourthReviewStopDeadline) retire(_ context.Context, _ int, _ bool, notAfter time.Time, _ bool) error {
	f.notAfter = notAfter
	return io.EOF
}

func TestFourthReviewFreshnessStartsAtRead(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old(), 2: old()})
	ops := &fourthReviewStopDeadline{fakeRollout: f}
	plan := f.plan(1)
	slots := map[int]*replica{}
	for _, r := range plan.Present {
		slots[r.index] = &r
	}
	readAt := f.now
	f.onHealth = func(string) { readAt = f.now }
	clockReads := 0
	plan.Now = func() time.Time {
		clockReads++
		if clockReads == 3 {
			// The scan finished at the second clock read, then the process
			// was suspended before handing the decision to retire.
			f.now = f.now.Add(time.Minute)
		}
		return f.now
	}
	f.replicas[1].drained = true
	f.replicas[1].drainBy = plan.RunID
	err := rolloutFinalRetire(t.Context(), ops, plan, slots, 1, false, func() error { return nil })
	require.ErrorIs(t, err, io.EOF)
	// Adapted: the stop's freshness counts from the end of the round, with a
	// budget of its own (rolloutStopFresh), since the health-check marker is
	// written before the round; still never from when the round was handed on.
	require.LessOrEqual(t, ops.notAfter.Sub(readAt), rolloutStopFresh)
}
