package project

import (
	"testing"

	"github.com/compose-spec/compose-go/v2/types"
	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/client"
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
