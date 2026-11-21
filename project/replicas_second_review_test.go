package project

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/compose-spec/compose-go/v2/types"
	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/client"
	"github.com/lxc/incus-compose/shared"
)

func TestSecondReviewExpandedHealth(t *testing.T) {
	for _, key := range []string{"profile drain", "profile check without a verdict"} {
		t.Run(key, func(t *testing.T) {
			old := api.Instance{Name: "web-1", StatusCode: api.Running, Config: reviewLabeled(api.ConfigMap{
				client.ConfigHashKey: "old", shared.HealthStatusKey: shared.HealthStatusHealthy,
			})}
			r := api.Instance{Name: "web-2", StatusCode: api.Running, Config: reviewLabeled(api.ConfigMap{
				client.ConfigHashKey: "new",
			})}
			r.ExpandedConfig = api.ConfigMap{shared.HealthEnabledKey: "true", shared.HealthKeyPrefix + "test": `["CMD","true"]`}
			if key == "profile drain" {
				r.ExpandedConfig[shared.DNSDrainKey] = "true"
				r.Config[shared.HealthStatusKey] = shared.HealthStatusHealthy
			}
			for k, v := range r.Config {
				r.ExpandedConfig[k] = v
			}
			rs := serviceReplicas([]api.InstanceFull{{Instance: old}, {Instance: r}}, "p", "web", "new")
			_, drop := reconcileReplicas(rs, 1)
			require.False(t, drop[1], "drops the only serving replica: %v", drop)
		})
	}
}

func TestSecondReviewLongIndex(t *testing.T) {
	service := strings.Repeat("a", 62)
	const index = 1025
	name := client.SanitizeIncusName(fmt.Sprintf("%s-%d", service, index), -1)
	require.Equal(t, index, replicaIndex(name, service))
}

func TestSecondReviewOccupiedHole(t *testing.T) {
	owned := api.Instance{Name: "web-2", Config: reviewLabeled(api.ConfigMap{})}
	foreign := api.Instance{Name: "web-1"}
	rs := serviceReplicas([]api.InstanceFull{{Instance: owned}, {Instance: foreign}}, "p", "web", "")
	indexes, _ := reconcileReplicas(rs, 2)
	require.Equal(t, []int{2, 3}, indexes, "web-1 already exists and belongs to someone else")
}

func TestSecondReviewBuiltTarget(t *testing.T) {
	c := client.NewOfflineClient(t.Context(), "p")
	p := New()
	s := types.ServiceConfig{Name: "web", Build: &types.BuildConfig{Context: "."}}
	p.Project = &types.Project{Name: "p", Services: types.Services{"web": s}}
	_, err := p.Resources(c)
	require.NoError(t, err)
	// up's pull skips this buildable image. Its existing alias in Incus
	// therefore has no state on the client when the second plan ranks replicas.
	cfg := func(name, fp string) api.InstanceFull {
		return api.InstanceFull{Instance: api.Instance{Name: name, StatusCode: api.Running, Config: reviewLabeled(api.ConfigMap{
			client.ConfigHashKey: "h", shared.HealthStatusKey: shared.HealthStatusHealthy, "volatile.base_image": fp,
		})}}
	}
	rs := serviceReplicas([]api.InstanceFull{cfg("web-1", "A"), cfg("web-2", "B")}, "p", "web", "h")

	// The image is not ensured, so the target is read from the project's alias
	// in Incus. An offline client has no Incus to read it from: the read is
	// stood in for, and answers B, as the alias in the scenario does.
	// (Adapted: the review called serviceImageTarget, which can only answer ""
	// offline.)
	res, err := c.Resource(client.KindImage, "localhost/web", nil)
	require.NoError(t, err)

	img := res.(*client.Image)
	require.False(t, img.IsEnsured())
	offline, err := serviceImageTarget(c, s)
	require.NoError(t, err)
	require.Empty(t, offline, "offline, no alias can be read")

	target, err := imageTarget(img, func() (string, error) { return "B", nil })
	require.NoError(t, err)
	markImage(rs, target)
	_, drop := reconcileReplicas(rs, 1)
	require.False(t, drop[2], "local alias points to B, drop=%v", drop)
}

func TestSecondReviewReusedSurvivor(t *testing.T) {
	c := client.NewOfflineClient(t.Context(), "p")
	p := New()
	p.Project = &types.Project{Name: "p", Services: types.Services{"web": {Name: "web", Image: "alpine"}}}
	before, err := p.Resources(c, ResourcesRecreating("web-1", "web-2"))
	require.NoError(t, err)
	r, err := c.Resource(client.KindInstance, "web-2", nil)
	require.NoError(t, err)
	require.Contains(t, before["web"], r)
	marked := reflect.ValueOf(r).Elem().FieldByName("deleteMarked")
	require.True(t, marked.Bool())
	after, err := p.Resources(c, ResourcesRecreating("web-2"))
	require.NoError(t, err)
	var kept []*client.Instance
	for _, res := range after["web"] {
		i, ok := res.(*client.Instance)
		if ok {
			kept = append(kept, i)
		}
	}
	require.Len(t, kept, 1)
	require.Same(t, r, kept[0])
	require.False(t, marked.Bool())
}

func TestSecondReviewRecreateHookOwners(t *testing.T) {
	c := client.NewOfflineClient(t.Context(), "p")
	p := New()
	scale := 2
	s := types.ServiceConfig{
		Name: "web", Image: "alpine", Scale: &scale,
		PreStart: []types.PreStartHook{{ContainerSpec: types.ContainerSpec{Command: types.ShellCommand{"migrate"}}}},
	}
	p.Project = &types.Project{Name: "p", Services: types.Services{"web": s}}
	before, err := p.Resources(c, ResourcesRecreating("web-2", "web-3"))
	require.NoError(t, err)
	owners := func(resources []client.Resource) []string {
		var names []string
		for _, res := range resources {
			inst, ok := res.(*client.Instance)
			if ok && len(inst.Config.PreRun) > 0 {
				names = append(names, inst.IncusName())
			}
		}
		return names
	}
	require.Equal(t, []string{"web-2"}, owners(before["web"]))
	after, err := p.Resources(c)
	require.NoError(t, err)
	require.Equal(t, []string{"web-1"}, owners(after["web"]))
}

func TestSecondReviewRequiredDependencyAtZero(t *testing.T) {
	c := client.NewOfflineClient(t.Context(), "p")
	p := New()
	zero := 0
	p.Project = &types.Project{Name: "p", Services: types.Services{
		"db": {Name: "db", Image: "alpine", Scale: &zero},
		"web": {Name: "web", Image: "alpine", DependsOn: types.DependsOnConfig{
			"db": {Condition: types.ServiceConditionHealthy, Required: true},
		}},
	}}
	rs, err := p.Resources(c, ResourcesAddReplica("web", 2, true))
	if err != nil {
		return
	}
	r := rs["web"][0].(*client.Instance)
	require.NotEmpty(t, r.Config.Dependencies, "a required db has no healthy instance and rollout has no dependency to check")
}
