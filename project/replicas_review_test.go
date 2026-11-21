package project

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/compose-spec/compose-go/v2/types"
	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/client"
	"github.com/lxc/incus-compose/shared"
)

// Regression tests from an adversarial review of how `up` reconciles a
// service on the replicas it has: each one is a replica `up` deleted that it
// should have kept, or kept a decision about after it no longer held.

// An instance named like a replica but without the labels, made by hand, is
// not one of the service's: `up` never takes it for a surplus to delete.
func TestReviewForeignInstance(t *testing.T) {
	managed := api.Instance{Name: "web-1", StatusCode: api.Running, Config: api.ConfigMap{ServiceLabelKey: "web", labelIncusComposePrefix + "project": "p", client.ConfigHashKey: "h"}}
	other := api.Instance{Name: "web-99", StatusCode: api.Running}
	rs := serviceReplicas([]api.InstanceFull{{Instance: managed}, {Instance: other}}, "p", "web", "h")
	_, drop := reconcileReplicas(rs, 1)
	if drop[99] {
		t.Fatal("unlabelled, independently created web-99 selected for deletion")
	}

	// It only holds its name, which no new replica takes.
	owned := ownedReplicas(rs)
	require.Len(t, owned, 1)
	assert.Equal(t, 1, owned[0].index)
}

// A service name long enough for its replicas' names to be a hash still has
// replicas. (The review's instance carried the service label alone; a replica
// carries the project's too.)
func TestReviewLongService(t *testing.T) {
	service := strings.Repeat("a", 62)
	inst := api.Instance{Name: client.SanitizeIncusName(service+"-1", -1), Config: api.ConfigMap{ServiceLabelKey: service, labelIncusComposePrefix + "project": "p"}}
	rs := serviceReplicas([]api.InstanceFull{{Instance: inst}}, "p", service, "")
	if len(rs) == 0 {
		t.Fatal("could not discover replica 1:", inst.Name)
	}

	assert.Equal(t, 1, rs[0].index)
	assert.Equal(t, 7, replicaIndex(client.SanitizeIncusName(service+"-7", -1), service))
	assert.Zero(t, replicaIndex(client.SanitizeIncusName("other-1", -1), service))
}

func reviewLabeled(cfg api.ConfigMap) api.ConfigMap {
	cfg[ServiceLabelKey] = "web"
	cfg[labelIncusComposePrefix+"project"] = "p"

	return cfg
}

// A replica with a healthcheck and no verdict yet does not serve: `up` keeps
// the old one that was called healthy rather than it. (The review's instances
// carried no labels, which no longer makes them replicas at all.)
func TestReviewMissingVerdict(t *testing.T) {
	a := api.Instance{Name: "web-1", StatusCode: api.Running, Config: reviewLabeled(api.ConfigMap{
		client.ConfigHashKey: "old", shared.HealthStatusKey: shared.HealthStatusHealthy,
	})}
	b := api.Instance{Name: "web-2", StatusCode: api.Running, Config: reviewLabeled(api.ConfigMap{
		client.ConfigHashKey: "new", shared.HealthEnabledKey: "true", shared.HealthKeyPrefix + "test": `["CMD","true"]`,
	})}
	rs := serviceReplicas([]api.InstanceFull{{Instance: a}, {Instance: b}}, "p", "web", "new")
	require.Len(t, rs, 2)

	_, drop := reconcileReplicas(rs, 1)
	if drop[1] {
		t.Fatal("drops the only replica with a healthy verdict", drop)
	}

	assert.True(t, drop[2])
}

// Of two replicas alike but for the image, the one made from the image the
// service points at now stays: `up` does not undo an image update a rollout
// cut short got halfway through.
func TestReviewImageOnlySurplus(t *testing.T) {
	instance := func(name, fp string) api.InstanceFull {
		return api.InstanceFull{Instance: api.Instance{Name: name, StatusCode: api.Running, Config: reviewLabeled(api.ConfigMap{
			client.ConfigHashKey: "h", shared.HealthStatusKey: shared.HealthStatusHealthy, "volatile.base_image": fp,
		})}}
	}
	rs := serviceReplicas([]api.InstanceFull{instance("web-1", "previous"), instance("web-2", "target")}, "p", "web", "h")
	markImage(rs, "target")

	_, drop := reconcileReplicas(rs, 1)
	if drop[2] {
		t.Fatal("up discards the healthy target image", drop)
	}

	assert.True(t, drop[1])

	// With no target known, the highest index goes, as before.
	markImage(rs, "")

	_, drop = reconcileReplicas(rs, 1)
	assert.True(t, drop[2])
}

// `up` plans twice on one client, to pull and to ensure: a replica the first
// plan marked as surplus and the second keeps is not deleted.
func TestReviewStickyScaleDown(t *testing.T) {
	c := client.NewOfflineClient(t.Context(), "test")
	p := New()
	p.Project = &types.Project{Name: "test", Services: types.Services{"web": {Name: "web", Image: "alpine"}}}
	_, err := p.Resources(c, ResourcesRecreating("web-1", "web-2"))
	if err != nil {
		t.Fatal(err)
	}
	resources, err := p.Resources(c, ResourcesRecreating("web-2"))
	if err != nil {
		t.Fatal(err)
	}
	for _, res := range resources["web"] {
		i, ok := res.(*client.Instance)
		if ok && reflect.ValueOf(i).Elem().FieldByName("deleteMarked").Bool() {
			t.Fatal("the only remaining instance is marked for deletion:", i.IncusName())
		}
	}
}

// A dependent planned again waits for the replicas of its dependency this
// plan keeps, not for the ones an earlier plan on the same client saw.
func TestReviewDependencyAfterRecreate(t *testing.T) {
	c := client.NewOfflineClient(t.Context(), "shop")
	p := New()
	p.Project = &types.Project{Name: "shop", Services: types.Services{
		"db":  {Name: "db", Image: "alpine"},
		"api": {Name: "api", Image: "alpine", DependsOn: types.DependsOnConfig{"db": {Condition: types.ServiceConditionHealthy}}},
	}}
	_, err := p.Resources(c, ResourcesRecreating("db-2", "api-1"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := p.Resources(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res["api"] {
		i, ok := r.(*client.Instance)
		if ok && i.Config.Dependencies["db-1"] != shared.HealthStatusHealthy {
			t.Fatalf("db is now %s, api still waits on %v", res["db"][0].IncusName(), i.Config.Dependencies)
		}

		if ok {
			assert.Equal(t, map[string]string{"db-1": shared.HealthStatusHealthy}, i.Config.Dependencies)
		}
	}
}

// `scale:` with no deploy block declares the replicas as deploy.replicas does.
func TestReviewComposeScale(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "compose.yaml"), []byte("name: shop\nservices:\n  web:\n    image: alpine\n    scale: 2\n"), 0600)
	if err != nil {
		t.Fatal(err)
	}
	p, err := New().Load(t.Context(), LoadWorkingDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	s := p.Services["web"]
	if s.GetScale() != 2 {
		t.Fatal("fixture has no scale")
	}
	plans, err := p.replicaPlans(client.NewOfflineClient(t.Context(), "shop"), &ResourcesOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plans["web"].indexes) != s.GetScale() {
		t.Fatalf("planned indexes=%v, compose declares %d, deploy=%v", plans["web"].indexes, s.GetScale(), s.Deploy)
	}
}

// One replica with no placement co-locates on the connected member only when
// a restricted project may use it; else it goes to a member it may use.
func TestReviewSingleRestrictedPlacement(t *testing.T) {
	env := clusterPlacementEnv{clustered: true, remotePool: true, networkType: "bridge", serverName: "node1",
		members: membersInGroups(testClusterMembers(), []string{"compute"})}
	_, targets, _, err := resolveServicePlacementEnv(env, types.ServiceConfig{Name: "web"}, nil, false)
	if err != nil || len(targets) != 1 || targets[0] != "node3" {
		t.Fatalf("allowed=node3, targets=%v, err=%v", targets, err)
	}

	env.members = testClusterMembers()
	_, targets, _, err = resolveServicePlacementEnv(env, types.ServiceConfig{Name: "web"}, nil, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"node1"}, targets, "unrestricted, it stays on the connected member")
}

// An instance deleted to be recreated keeps its name: the stand-in listed for
// it counts as one of the service's replicas although it carries no labels.
func TestReviewRecreatingKeepsItsName(t *testing.T) {
	c := client.NewOfflineClient(t.Context(), "test")
	p := New()
	p.Project = &types.Project{Name: "test", Services: types.Services{"web": {Name: "web", Image: "alpine"}}}

	plans, err := p.replicaPlans(c, &ResourcesOptions{recreating: []string{"web-2"}})
	require.NoError(t, err)
	assert.Equal(t, []int{2}, plans["web"].indexes)
}
