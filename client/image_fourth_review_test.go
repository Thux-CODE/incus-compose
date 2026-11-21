package client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/iclient"
)

func TestFourthReviewImageReadEOF(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		// Close the request with no API response body.
	}))
	t.Cleanup(srv.Close)
	conn, err := iclient.NewConnection(&iclient.ConfigRemoteInfo{Addrs: []string{srv.URL}})
	require.NoError(t, err)

	g := New(t.Context())
	g.incus = conn
	c, err := g.newProjectClient("shop", "shop", false, nil)
	require.NoError(t, err)

	res, err := c.Resource(KindImage, "example.invalid/web", &ImageConfig{
		Build: &BuildConfig{Context: "."}, Platform: "linux/amd64",
	})
	require.NoError(t, err)

	t.Run("project alias during dry run", func(t *testing.T) {
		err := res.(*Image).Ensure(t.Context())
		require.ErrorIs(t, err, io.EOF)
		require.NotErrorIs(t, err, ErrNotFound)
		require.NotErrorIs(t, err, ErrCreate)
	})

	t.Run("project alias with build disabled", func(t *testing.T) {
		err := res.(*Image).Ensure(t.Context(), OptionCreate(), OptionBuild(BuildInfo{Mode: BuildNever}))
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("server architecture during dry run", func(t *testing.T) {
		img, err := c.Resource(KindImage, "example.invalid/other", &ImageConfig{})
		require.NoError(t, err)
		err = img.(*Image).Ensure(t.Context())
		require.ErrorIs(t, err, io.EOF)
		require.NotErrorIs(t, err, ErrCreate, "rolloutPrepare accepts ErrCreate as an absent image")
	})
}
