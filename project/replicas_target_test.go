package project

import (
	"context"
	"testing"

	"github.com/compose-spec/compose-go/v2/types"
	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/client"
	"github.com/lxc/incus-compose/shared"
)

// A built service's image is not ensured when up plans its surplus:
// serviceImageTarget reads the project's alias of it, and the replica on the
// image the alias points at stays. Not parallel: it stands in for the read.
func TestServiceImageTargetReadsTheProjectAlias(t *testing.T) {
	c := client.NewOfflineClient(t.Context(), "p")
	p := New()
	s := types.ServiceConfig{Name: "web", Build: &types.BuildConfig{Context: "."}}
	p.Project = &types.Project{Name: "p", Services: types.Services{"web": s}}

	_, err := p.Resources(c)
	require.NoError(t, err)

	var asked []string

	saved := readAliasTarget
	t.Cleanup(func() { readAliasTarget = saved })

	readAliasTarget = func(_ context.Context, img *client.Image) (string, error) {
		asked = append(asked, img.IncusName())
		return "B", nil
	}

	target, err := serviceImageTarget(c, s)
	require.NoError(t, err)
	assert.Equal(t, "B", target)

	res, err := c.Resource(client.KindImage, "localhost/web", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{res.IncusName()}, asked, "the alias read is the project's image's")

	instance := func(name, fp string) api.InstanceFull {
		return api.InstanceFull{Instance: api.Instance{Name: name, StatusCode: api.Running, Config: reviewLabeled(api.ConfigMap{
			client.ConfigHashKey: "h", shared.HealthStatusKey: shared.HealthStatusHealthy, "volatile.base_image": fp,
		})}}
	}

	rs := serviceReplicas([]api.InstanceFull{instance("web-1", "A"), instance("web-2", "B")}, "p", "web", "h")
	markImage(rs, target)

	_, drop := reconcileReplicas(rs, 1)
	assert.True(t, drop[1])
	assert.False(t, drop[2])
}
