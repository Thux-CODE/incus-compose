package dns

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/iclient"
	"github.com/lxc/incus-compose/ievent/enricher"
	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/ievent/source"
)

// stalledIncus is an Incus that takes every request and answers none of them
// until the test is over: the slowest a daemon can be, and so a first run that
// never ends. Over a unix socket, which is the one transport needing no
// certificate.
func stalledIncus(t *testing.T) *iclient.Connection {
	t.Helper()

	// Short and under /tmp: a socket path is capped near a hundred bytes, which
	// a test's own temporary directory can overrun.
	dir, err := os.MkdirTemp("", "ic-dns-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	socket := filepath.Join(dir, "incus.sock")

	listener, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	require.NoError(t, err)

	release := make(chan struct{})

	srv := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}),
	}

	go func() { _ = srv.Serve(listener) }()

	t.Cleanup(func() {
		close(release)
		_ = srv.Close()
	})

	conn, err := iclient.NewConnection(&iclient.ConfigRemoteInfo{Name: "stalled", Addrs: []string{"unix:" + socket}})
	require.NoError(t, err)

	return conn
}

// TestARestartAnswersBeforeIncusDoes is a restart end to end: the cold store a
// previous run left, the source, the enricher and this plugin wired the way
// main wires them, and an Incus that never answers. The names have to be
// answered off the store anyway; they used to fall through to the forwarders
// until the first run had been all the way round, which against a slow daemon
// is tens of seconds.
//
// Asked through the engine rather than a socket, so nothing here races for a
// port.
func TestARestartAnswersBeforeIncusDoes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store := filepath.Join(dir, "enricher-cold-store.json")

	net0 := iutil.NewNetwork("net0", "shop", true, "10.0.0.1/24", "")

	held := map[string]any{
		"version": 1,
		"projects": map[string]any{
			"shop": map[string]any{
				"config": map[string]string{},
				"instances": map[string]*iutil.Instance{
					"web": iutil.NewInstance(true, map[string]string{}, []iutil.InstanceInterface{
						iutil.NewInstanceInterface("shop", "net0", true, []string{"10.0.0.5"}, nil),
					}, map[string]*iutil.Network{iutil.NetworkKey("shop", "net0"): net0}),
				},
				"networks": map[string]*iutil.Network{"net0": net0},
			},
		},
	}

	b, err := json.Marshal(held)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(store, b, 0o600))

	enr := enricher.New(slog.Default(),
		enricher.ReadTimeout(time.Minute),
		enricher.StoreFile(store),
	)

	p := New(slog.Default(), Suffix("incus"), ColdDir(dir))

	src, err := source.New(slog.Default(), stalledIncus(t), []iutil.Plugin{enr, p})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = src.Run(ctx) }()
	go func() { _ = enr.Run(ctx) }()
	go func() { _ = p.Run(ctx) }()

	// Asked from inside the network, the way a resolver relaying for one of
	// the instances does.
	q := query("web.shop.incus.")

	opt := q.IsEdns0()
	opt.Option = append(opt.Option, &dns.EDNS0_SUBNET{
		Code:          dns.EDNS0SUBNET,
		Family:        1,
		SourceNetmask: 32,
		Address:       net.ParseIP("10.0.0.5").To4(),
	})

	var answer []string

	require.Eventually(t, func() bool {
		w := &recorder{}

		handled, _, err := p.view.Answer(t.Context(), w, q)
		if err != nil || !handled || w.msg == nil {
			return false
		}

		answer = nil

		for _, rr := range w.msg.Answer {
			a, ok := rr.(*dns.A)
			if ok {
				answer = append(answer, a.A.String())
			}
		}

		return len(answer) > 0
	}, 5*time.Second, 10*time.Millisecond, "a restart answered nothing while Incus had not answered")

	assert.Equal(t, []string{"10.0.0.5"}, answer)
	assert.False(t, p.view.Ready(), "a fleet nothing has read was called ready")
}
