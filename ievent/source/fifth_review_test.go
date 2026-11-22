package source

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ihttp "github.com/lxc/incus-compose/ievent/http"
	"github.com/lxc/incus-compose/ievent/iutil"
)

func TestFifthReadinessWithOlderRoundCompletions(t *testing.T) {
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	h := ihttp.New(slog.Default(), ihttp.Listen(addr))
	s, err := New(slog.Default(), nil, []iutil.Plugin{h})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	defer func() { cancel(); <-done }()
	client := http.Client{Timeout: time.Second}
	status := func() int {
		res, err := client.Get("http://" + addr + "/ready")
		if err != nil {
			return 0
		}
		res.Body.Close()
		return res.StatusCode
	}
	s.sessionID, s.open = 1, true
	end := iutil.Command{Action: iutil.ActionSweepEnd, ChainState: iutil.ChainWarm, Session: 1}
	s.apply(end)
	require.Eventually(t, func() bool { return status() == 200 }, time.Second, time.Millisecond)
	s.chain = iutil.ChainCold
	s.hand(iutil.NewEvent(time.Now(), iutil.ActionDisconnected, "", "", ""))
	s.sessionID = 2
	s.hand(iutil.NewEvent(time.Now(), iutil.ActionConnected, "", "", ""))
	s.hand(iutil.NewEvent(time.Now(), iutil.ActionDisconnected, "", "", ""))
	// Listener 3 opened, but its Connected never left the enricher.
	s.sessionID = 3
	end.Session = 2
	for range 4 {
		s.apply(end)
		require.Equal(t, 503, status())
	}
	end.Session = 3
	s.apply(end)
	require.Equal(t, 200, status())
}
