package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllowedClusterGroups(t *testing.T) {
	t.Parallel()

	groups := func(config map[string]string) []string {
		return (&Client{projectConfig: config}).AllowedClusterGroups()
	}

	assert.Nil(t, groups(nil), "no project config: every member")
	assert.Nil(t, groups(map[string]string{"restricted.cluster.groups": "s1-i1"}), "the key counts only on a restricted project")
	assert.Nil(t, groups(map[string]string{"restricted": "true"}), "restricted without groups: every member")
	assert.Equal(t, []string{"s1-i1"}, groups(map[string]string{"restricted": "true", "restricted.cluster.groups": "s1-i1"}))
	assert.Equal(t, []string{"a", "b"}, groups(map[string]string{"restricted": "true", "restricted.cluster.groups": "a, b,"}))
}
