package main

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/compose-spec/compose-go/v2/types"
	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/client"
	"github.com/lxc/incus-compose/shared"
)

// Regression tests from an adversarial review of rollout: each one is a
// sequence that left the service with fewer healthy replicas than declared,
// or retired a replica on a read that could not tell.

func reviewCurrent() *fakeReplica {
	r := old()
	r.fresh, r.current = true, true
	return r
}

// Picked up again with the new replica unhealthy and the old one drained, the
// old one is the only one that serves: it is put back and the new one given
// back. That is a rollback, and the run ends with an error that says so. The
// review asked for the drain to be kept, which leaves nothing serving; the put
// back stays, and the error is what makes it visible.
func TestReviewDrainedOnResume(t *testing.T) {
	oldOne := old()
	oldOne.drained = true
	oldOne.drainBy = "prev"
	newOne := reviewCurrent()
	newOne.statuses = []string{shared.HealthStatusUnhealthy}
	f := newFakeRollout(t, map[int]*fakeReplica{1: oldOne, 2: newOne})
	plan := f.plan(1)

	err := rollout(t.Context(), f, plan)
	require.ErrorIs(t, err, errRolloutUnhealthy)

	assert.Equal(t, []string{"undrain web-1", "drain web-2", "retire web-2"}, f.events)
	assert.Equal(t, []string{"web-1 old"}, f.final())
	assert.Contains(t, plan.Out.(*bytes.Buffer).String(), "web-1: drained by an earlier rollout, put back: without it 0 of 1 declared would serve")
}

// Three failed reads of a replica an earlier run made are no verdict: the run
// stops and retires nothing, where it used to take the replica for old and
// retire it on the index.
func TestReviewResumeReadFailure(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: reviewCurrent(), 2: old()})
	f.minReady = 1
	f.healthErrs = rolloutHealthReads

	err := rollout(t.Context(), f, f.plan(1))
	require.ErrorIs(t, err, errRolloutUnread)

	require.NotNil(t, f.replicas[1])
	require.NotNil(t, f.replicas[2])
	assert.Empty(t, f.events)
}

// With two replicas, an old one that turns unhealthy while the new one gets
// healthy is the one to go: the old healthy one and the new one serve.
func TestReviewHealthyBudget(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old(), 2: old()}, slowStart, slowStart)
	f.onHealth = func(s string) {
		if s == shared.HealthStatusHealthy {
			f.replicas[2].seen = shared.HealthStatusUnhealthy
			f.replicas[2].statuses = []string{shared.HealthStatusUnhealthy}
			f.onHealth = nil
		}
	}

	err := rollout(t.Context(), f, f.plan(2))
	require.NoError(t, err)

	assert.Equal(t, []string{
		"create web-3", "drain web-2", "retire web-2",
		"create web-2", "drain web-1", "retire web-1",
	}, f.events)
}

// The new replica restarted during the grace of the old one's drain: the
// stop is put off, the old one put back, the new one, which no longer serves,
// taken away, and the run fails.
func TestReviewRestartDuringGrace(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	plan := f.plan(1)
	f.onSleep = func(d time.Duration) {
		if d == plan.Grace {
			f.replicas[2].seen = shared.HealthStatusStarting
			f.replicas[2].statuses = []string{shared.HealthStatusStarting}
		}
	}

	err := rollout(t.Context(), f, plan)
	require.ErrorIs(t, err, errRolloutShort)
	assert.Contains(t, err.Error(), "web-1 not retired")

	assert.Equal(t, []string{"create web-2", "drain web-1", "undrain web-1", "discard web-2"}, f.events)
	assert.Equal(t, []string{"web-1 old"}, f.final())
	assert.Equal(t, 1, f.ready())
}

// An up to date replica that reads healthy, then unhealthy as the surplus is
// read, is old again: it is given back, the old one serves on, and the run
// fails.
func TestReviewSurplusAfterHealthChange(t *testing.T) {
	r := reviewCurrent()
	r.statuses = []string{shared.HealthStatusHealthy, shared.HealthStatusUnhealthy}
	f := newFakeRollout(t, map[int]*fakeReplica{1: old(), 2: r})
	f.minReady = 1

	err := rollout(t.Context(), f, f.plan(1))
	require.ErrorIs(t, err, errRolloutLapsed)

	assert.Equal(t, []string{"drain web-2", "retire web-2"}, f.events)
	assert.Equal(t, []string{"web-1 old"}, f.final())
}

// A rollout of an unchanged service with one replica, killed after the drain:
// the drained one is the old one, and run again it is retired with no create.
func TestReviewAllCurrent(t *testing.T) {
	oldOne := reviewCurrent()
	oldOne.fresh = false
	oldOne.drained = true
	oldOne.drainBy = "prev"
	f := newFakeRollout(t, map[int]*fakeReplica{1: oldOne, 2: reviewCurrent()}, slowStart)
	plan := f.plan(1)

	var dry bytes.Buffer

	described := plan
	described.Out = &dry
	rolloutDescribe(described)
	assert.Contains(t, dry.String(), "web-2 (running, up to date); 0 creates")

	err := rollout(t.Context(), f, plan)
	if err != nil || f.creates != 0 || f.replicas[2] == nil {
		t.Fatalf("creates=%d, events=%v, err=%v", f.creates, f.events, err)
	}

	assert.Equal(t, []string{"drain web-1", "retire web-1"}, f.events)
}

// A surplus of current replicas with no mark, as `up` or a rollout from
// before the marks leaves: the newest serves on, the oldest is the surplus
// given back, and nothing is created. (The second review pointed out that
// the first version of this test replaced both, against the promise that a
// run again never retires a replica already new and healthy.)
func TestReviewAllCurrentNoneDrained(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: {current: true, running: true, statuses: []string{shared.HealthStatusHealthy}}, 2: {current: true, running: true, statuses: []string{shared.HealthStatusHealthy}}}, slowStart)
	f.minReady = 1

	err := rollout(t.Context(), f, f.plan(1))
	require.NoError(t, err)

	assert.Equal(t, []string{"drain web-1", "retire web-1"}, f.events)
	assert.Equal(t, []string{"web-2 old"}, f.final())
	assert.Zero(t, f.creates)
}

// The service-wide pre_start hooks ran with the replica an earlier run made:
// picked up again, the rollout does not run them a second time.
func TestReviewOnceOnResume(t *testing.T) {
	ran := reviewCurrent()
	ran.preStart = "prev"
	f := newFakeRollout(t, map[int]*fakeReplica{3: ran, 2: old()}, slowStart)
	err := rollout(t.Context(), f, f.plan(2))
	if err != nil || len(f.firsts) != 0 {
		t.Fatalf("pre_start repeated on %v, err=%v", f.firsts, err)
	}

	assert.Equal(t, 1, f.creates)
}

// A replica is current only next to a target fingerprint it matches: with no
// alias read, its image is not known to be the one a create makes.
func TestReviewMissingTarget(t *testing.T) {
	cfg := map[string]string{client.ConfigHashKey: "h", "volatile.base_image": "old"}
	if rolloutCurrent(cfg, "h", nil) {
		t.Fatal("current with no target fingerprint")
	}
}

// A service whose new replicas would be born drained is refused: one would
// pass its check while ic-dns keeps it out of the service name.
func TestReviewDrainedReplacement(t *testing.T) {
	service := types.ServiceConfig{Name: "web", Labels: types.Labels{"dns.drain": "true"}}
	health := map[string]string{shared.HealthEnabledKey: "true", shared.HealthKeyPrefix + "test": `["CMD","true"]`}
	if len(rolloutBlockers(service, health, nil, nil, nil, false)) == 0 {
		t.Fatal("accepts a replacement excluded from service DNS")
	}
}

// A kill call that takes longer than the time left does not buy another
// round: the deadline is checked after it, on a status read then.
func TestReviewStopDeadline(t *testing.T) {
	kills := 0
	kill := func(context.Context) error {
		kills++
		time.Sleep(5 * time.Millisecond)
		return errors.New("timeout")
	}
	status := func(context.Context) (api.StatusCode, error) {
		if kills == 3 {
			return api.Stopped, nil
		}
		return api.Running, nil
	}
	err := rolloutStop(t.Context(), nil, kill, status, false, 2*time.Millisecond, time.Millisecond, time.Millisecond)
	if kills > 1 || err == nil {
		t.Fatalf("%d stop calls, err=%v despite expired deadline", kills, err)
	}

	assert.Contains(t, err.Error(), "still Running")
}

// An interrupt during the grace of a replica drained by hand, a drain no
// rollout marked, leaves it drained: it is neither set again nor taken off.
func TestReviewKeepsAnEarlierDrain(t *testing.T) {
	oldOne := old()
	oldOne.drained = true
	f := newFakeRollout(t, map[int]*fakeReplica{1: oldOne, 2: reviewCurrent()})
	plan := f.plan(1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	f.onSleep = func(d time.Duration) {
		if d == plan.Grace {
			cancel()
		}
	}

	err := rollout(ctx, f, plan)
	require.ErrorIs(t, err, context.Canceled)

	assert.Empty(t, f.events)
	assert.Equal(t, []string{"web-1 old drained", "web-2 new"}, f.final())
}
