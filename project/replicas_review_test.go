package project

import (
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

	require.Len(t, rs, 1)
	assert.Equal(t, 1, rs[0].index)
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
