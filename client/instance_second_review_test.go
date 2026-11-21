package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSecondReviewReadErrorClassification(t *testing.T) {
	g := New(t.Context())
	c, err := g.newProjectClient("p", "p", false, nil)
	require.NoError(t, err)
	r, err := c.Resource(KindInstance, "web-2", &InstanceConfig{})
	require.NoError(t, err)
	err = RunAction(t.Context(), r, ActionEnsure)
	require.ErrorIs(t, err, ErrDisconnected)
	require.NotErrorIs(t, err, ErrNotFound)
}
