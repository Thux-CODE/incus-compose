package dns

import (
	"testing"

	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"

	"github.com/lxc/incus-compose/ievent/iutil"
)

func TestADrainedReplicaLeavesItsService(t *testing.T) {
	t.Parallel()

	const started = incusapi.EventLifecycleInstanceStarted

	tests := []struct {
		name string
		feed []*iutil.Event
		want []resolves
	}{
		{
			// A drained replica is out of its service and still at its own
			// name, which is what a rolling update stops sending traffic to
			// while it can still be reached and checked.
			name: "a drained replica leaves its service and keeps its own name",
			feed: []*iutil.Event{
				eventOn(started, "shop", "web", map[string]string{metaService: "frontend"}, map[string]string{"net-a": "10.0.0.5"}),
				eventOn(started, "shop", "api", map[string]string{metaService: "frontend"}, map[string]string{"net-a": "10.0.0.6"}),
				eventOn(incusapi.EventLifecycleInstanceUpdated, "shop", "web",
					map[string]string{metaService: "frontend", metaDrain: "true"}, map[string]string{"net-a": "10.0.0.5"}),
			},
			want: []resolves{
				{qname: "frontend.shop.example.", from: "10.0.0.6", want: []string{"10.0.0.6"}},
				{qname: "web.shop.example.", from: "10.0.0.6", want: []string{"10.0.0.5"}},
			},
		},
		{
			name: "undraining a replica puts it back in its service",
			feed: []*iutil.Event{
				eventOn(started, "shop", "web", map[string]string{metaService: "frontend", metaDrain: "true"}, map[string]string{"net-a": "10.0.0.5"}),
				eventOn(started, "shop", "api", map[string]string{metaService: "frontend"}, map[string]string{"net-a": "10.0.0.6"}),
				eventOn(incusapi.EventLifecycleInstanceUpdated, "shop", "web",
					map[string]string{metaService: "frontend", metaDrain: "false"}, map[string]string{"net-a": "10.0.0.5"}),
			},
			want: []resolves{
				{qname: "frontend.shop.example.", from: "10.0.0.6", want: []string{"10.0.0.5", "10.0.0.6"}},
			},
		},
		{
			// Removing the key is the other way back, and the usual one.
			name: "removing the drain label puts it back in its service",
			feed: []*iutil.Event{
				eventOn(started, "shop", "web", map[string]string{metaService: "frontend", metaDrain: "true"}, map[string]string{"net-a": "10.0.0.5"}),
				eventOn(incusapi.EventLifecycleInstanceUpdated, "shop", "web",
					map[string]string{metaService: "frontend"}, map[string]string{"net-a": "10.0.0.5"}),
			},
			want: []resolves{
				{qname: "frontend.shop.example.", from: "10.0.0.5", want: []string{"10.0.0.5"}},
			},
		},
		{
			// Draining the last replica leaves nothing to answer with: a drain is
			// an operator's call, and second-guessing it would send traffic to
			// the instance it was meant to keep traffic away from.
			name: "draining every replica leaves the service unanswered",
			feed: []*iutil.Event{
				eventOn(started, "shop", "web", map[string]string{metaService: "frontend", metaDrain: "true"}, map[string]string{"net-a": "10.0.0.5"}),
			},
			want: []resolves{
				{qname: "frontend.shop.example.", from: "10.0.0.5"},
				{qname: "web.shop.example.", from: "10.0.0.5", want: []string{"10.0.0.5"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			snap := fed(t, test.feed...).state.snapshot()

			for _, want := range test.want {
				assert.Equal(t, want.want, answered(t, snap, want.qname, want.from),
					"%s asked from %s", want.qname, want.from)
			}
		})
	}
}
