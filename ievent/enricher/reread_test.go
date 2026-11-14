package enricher

import (
	"testing"
	"time"

	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// settled waits until every event handed in so far has been taken: a read for
// another key, sent last, only goes out once everything before it has been
// folded in, joined or not.
func settled(t *testing.T, h *fixture) {
	t.Helper()

	h.send(incusapi.EventLifecycleInstanceUpdated, "p", "sync")
	require.Eventually(t, func() bool { return h.readsOf("p", "sync") == 1 },
		time.Second, time.Millisecond, "the events before it were not taken")
}

// tagOf is the tag a read carried, which is what the fixture changes to make
// the next read say something new.
func tagOf(ev *iutil.Event) string {
	tag, _ := ev.Instance().ConfigValue("user.label." + testlib.LabelPrefix + "tag")

	return tag
}

// TestAnEventDuringAReadIsReadAgain: a read takes the instance as it was when
// it went out. An event announcing a change after that - a label set right
// after a start, while the start is still waiting for its address - cannot be
// answered by it, or the change is lost until something else moves.
func TestAnEventDuringAReadIsReadAgain(t *testing.T) {
	t.Parallel()

	h := seeded(t, 0, 0)

	h.mu.Lock()
	h.gate = make(chan struct{})
	h.tag = "before"
	gate := h.gate
	h.mu.Unlock()

	h.send(incusapi.EventLifecycleInstanceStarted, "p", "web")
	require.Eventually(t, func() bool { return h.readsOf("p", "web") == 1 },
		time.Second, time.Millisecond, "the first read went out")

	// The change lands after the read went out, and its event with it.
	h.mu.Lock()
	h.tag = "after"
	h.mu.Unlock()

	h.send(incusapi.EventLifecycleInstanceUpdated, "p", "web")
	settled(t, h)

	close(gate)

	first := h.next()
	assert.Equal(t, incusapi.EventLifecycleInstanceStarted, first.Action())
	assert.Equal(t, "before", tagOf(first), "the first read is what it was")

	second := h.next()
	assert.Equal(t, incusapi.EventLifecycleInstanceUpdated, second.Action())
	assert.Equal(t, "after", tagOf(second), "the update carried a read taken before it")
	assert.Equal(t, 2, h.readsOf("p", "web"))
	assert.Equal(t, "sync", h.next().Name())
}

// TestEventsDuringOneReadShareTheNextOne: however many arrive while a read is
// out, they wait for one read between them, not one each.
func TestEventsDuringOneReadShareTheNextOne(t *testing.T) {
	t.Parallel()

	h := seeded(t, 0, 0)

	h.mu.Lock()
	h.gate = make(chan struct{})
	h.tag = "before"
	gate := h.gate
	h.mu.Unlock()

	h.send(incusapi.EventLifecycleInstanceUpdated, "p", "web")
	require.Eventually(t, func() bool { return h.readsOf("p", "web") == 1 },
		time.Second, time.Millisecond, "the first read went out")

	h.mu.Lock()
	h.tag = "after"
	h.mu.Unlock()

	for range 3 {
		h.send(incusapi.EventLifecycleInstanceUpdated, "p", "web")
	}

	settled(t, h)

	close(gate)

	assert.Equal(t, "before", tagOf(h.next()))
	assert.Equal(t, "after", tagOf(h.next()))
	assert.Equal(t, "sync", h.next().Name(), "the ones that said nothing new walked")
	assert.Equal(t, 2, h.readsOf("p", "web"), "three events cost more than one read")
}
