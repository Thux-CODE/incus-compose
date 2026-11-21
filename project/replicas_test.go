package project

import (
	"testing"

	incusApi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"

	"github.com/lxc/incus-compose/client"
	"github.com/lxc/incus-compose/shared"
)

// up is a replica as `up` finds one that serves: running, current, healthy.
func up(index int) replica {
	return replica{index: index, running: true, current: true, healthy: true}
}

func TestReconcileReplicas(t *testing.T) {
	t.Parallel()

	stale := func(r replica) replica { r.current = false; return r }
	stopped := func(r replica) replica { r.running = false; r.healthy = false; return r }
	drained := func(r replica) replica { r.drained = true; r.healthy = false; return r }
	starting := func(r replica) replica { r.healthy = false; return r }
	unhashed := func(r replica) replica { r.unhashed = true; return r }
	otherImage := func(r replica) replica { r.otherImage = true; return r }

	for _, tt := range []struct {
		name     string
		existing []replica
		desired  int
		indexes  []int
		drop     []int
	}{
		{
			name:    "nothing there creates 1 to N",
			desired: 2,
			indexes: []int{1, 2},
		},
		{
			name:     "the same count is left alone, whatever the indexes",
			existing: []replica{up(2)},
			desired:  1,
			indexes:  []int{2},
		},
		{
			name:     "two replicas on 1 and 3 stay",
			existing: []replica{up(1), up(3)},
			desired:  2,
			indexes:  []int{1, 3},
		},
		{
			name:     "fewer take the lowest free indexes",
			existing: []replica{up(2)},
			desired:  3,
			indexes:  []int{1, 2, 3},
		},
		{
			name:     "fewer next to a gap fill it first",
			existing: []replica{up(1), up(3)},
			desired:  4,
			indexes:  []int{1, 2, 3, 4},
		},
		{
			name:     "more drop the highest index",
			existing: []replica{up(1), up(2), up(3)},
			desired:  2,
			indexes:  []int{1, 2, 3},
			drop:     []int{3},
		},
		{
			name:     "more drop a stopped one before a running one",
			existing: []replica{stopped(up(1)), up(2)},
			desired:  1,
			indexes:  []int{1, 2},
			drop:     []int{1},
		},
		{
			name:     "a drained one before one serving",
			existing: []replica{drained(up(1)), up(2)},
			desired:  1,
			indexes:  []int{1, 2},
			drop:     []int{1},
		},
		{
			name:     "a stale one before a current one",
			existing: []replica{stale(up(1)), up(2)},
			desired:  1,
			indexes:  []int{1, 2},
			drop:     []int{1},
		},
		{
			name:     "a stopped one before a drained one before a stale one",
			existing: []replica{stale(up(1)), drained(up(2)), stopped(up(3)), up(4)},
			desired:  2,
			indexes:  []int{1, 2, 3, 4},
			drop:     []int{2, 3},
		},
		{
			name:     "never the last healthy one: a rollout killed while the new one starts",
			existing: []replica{stale(up(1)), starting(up(2))},
			desired:  1,
			indexes:  []int{1, 2},
			drop:     []int{2},
		},
		{
			name:     "a healthy one stays even past a stopped one",
			existing: []replica{drained(up(1)), stopped(up(2)), up(3)},
			desired:  1,
			indexes:  []int{1, 2, 3},
			drop:     []int{1, 2},
		},
		{
			name:     "none healthy: the order alone decides",
			existing: []replica{starting(up(1)), starting(up(2))},
			desired:  1,
			indexes:  []int{1, 2},
			drop:     []int{2},
		},
		{
			name:     "as many healthy ones as declared stay, not one alone",
			existing: []replica{stale(up(1)), stale(up(2)), starting(up(3)), starting(up(4))},
			desired:  2,
			indexes:  []int{1, 2, 3, 4},
			drop:     []int{3, 4},
		},
		{
			name:     "fewer healthy ones than declared all stay",
			existing: []replica{stale(up(1)), starting(up(2)), starting(up(3))},
			desired:  2,
			indexes:  []int{1, 2, 3},
			drop:     []int{3},
		},
		{
			name:     "one with no config hash before one with the current hash",
			existing: []replica{up(1), unhashed(up(2))},
			desired:  1,
			indexes:  []int{1, 2},
			drop:     []int{2},
		},
		{
			name:     "one made from another image before one made from the service's",
			existing: []replica{up(1), otherImage(up(2))},
			desired:  1,
			indexes:  []int{1, 2},
			drop:     []int{2},
		},
		{
			name:     "scaled to 0, all go",
			existing: []replica{up(2), up(5)},
			desired:  0,
			indexes:  []int{2, 5},
			drop:     []int{2, 5},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			indexes, drop := reconcileReplicas(tt.existing, tt.desired)
			assert.Equal(t, tt.indexes, indexes)

			dropped := []int{}
			for _, i := range indexes {
				if drop[i] {
					dropped = append(dropped, i)
				}
			}

			want := tt.drop
			if want == nil {
				want = []int{}
			}

			assert.Equal(t, want, dropped)
		})
	}
}

func TestServiceReplicas(t *testing.T) {
	t.Parallel()

	inst := func(name string, status incusApi.StatusCode, config map[string]string) incusApi.InstanceFull {
		return incusApi.InstanceFull{Instance: incusApi.Instance{Name: name, StatusCode: status, Location: "i1", Config: config}}
	}

	labels := func(service string, extra ...string) map[string]string {
		m := map[string]string{ServiceLabelKey: service, labelIncusComposePrefix + "project": "app"}
		for i := 0; i+1 < len(extra); i += 2 {
			m[extra[i]] = extra[i+1]
		}

		return m
	}

	instances := []incusApi.InstanceFull{
		inst("web-3", incusApi.Running, labels("web", client.ConfigHashKey, "v1:new", shared.HealthStatusKey, shared.HealthStatusHealthy)),
		inst("web-1", incusApi.Running, labels("web", client.ConfigHashKey, "v1:old", shared.DNSDrainKey, "true")),
		inst("web-2", incusApi.Stopped, labels("web")),
		inst("web-4", incusApi.Running, labels("web", shared.HealthStatusKey, shared.HealthStatusStarting)),
		inst("web-10", incusApi.Running, nil),
		inst("web-pre-start-0", incusApi.Running, labels("web", OneOffKey, "true")),
		inst("web-7", incusApi.Running, labels("web", OneOffKey, "true")),
		inst("web-8", incusApi.Running, labels("api")),
		inst("web-9", incusApi.Running, map[string]string{ServiceLabelKey: "web", labelIncusComposePrefix + "project": "other"}),
		inst("webapp-1", incusApi.Running, labels("web")),
		inst("web-0", incusApi.Running, labels("web")),
		inst("api-1", incusApi.Running, labels("api")),
	}

	got := serviceReplicas(instances, "app", "web", "v1:new")

	assert.Equal(t, []replica{
		{index: 1, location: "i1", running: true, drained: true, current: false, healthy: false},
		{index: 2, location: "i1", running: false, current: true, unhashed: true, healthy: false},
		{index: 3, location: "i1", running: true, current: true, healthy: true},
		{index: 4, location: "i1", running: true, current: true, unhashed: true, healthy: false},
		{index: 7, foreign: true},
		{index: 8, foreign: true},
		{index: 9, foreign: true},
		{index: 10, foreign: true},
	}, got, "by label and name; a one-off, another service's or project's instance and web-10 with no labels at all only hold their names")
}

func TestReplicaTargets(t *testing.T) {
	t.Parallel()

	located := func(index int, location string) replica {
		r := up(index)
		r.location = location

		return r
	}

	t.Run("one that exists stays where it runs, a new one takes a member no other holds", func(t *testing.T) {
		t.Parallel()

		existing := []replica{located(2, "i2"), located(3, "i3")}
		got := replicaTargets([]int{1, 2, 3}, nil, existing, []string{"i1", "i2", "i3"}, true, "i1")
		assert.Equal(t, map[int]string{1: "i1", 2: "i2", 3: "i3"}, got)
	})

	t.Run("a dropped one frees nothing it holds", func(t *testing.T) {
		t.Parallel()

		existing := []replica{located(1, "i1"), located(2, "i1")}
		got := replicaTargets([]int{1, 2, 3}, map[int]bool{2: true}, existing, []string{"i1", "i2"}, true, "i1")
		assert.Equal(t, map[int]string{1: "i1", 2: "i1", 3: "i2"}, got)
	})

	t.Run("the scheduler's empty target stays empty", func(t *testing.T) {
		t.Parallel()

		existing := []replica{located(1, "i3")}
		got := replicaTargets([]int{1, 2}, nil, existing, []string{""}, true, "i1")
		assert.Equal(t, map[int]string{1: "i3", 2: ""}, got)
	})

	t.Run("not clustered, no targets", func(t *testing.T) {
		t.Parallel()

		got := replicaTargets([]int{2, 3}, nil, []replica{up(2)}, []string{""}, false, "")
		assert.Empty(t, got)
	})
}
