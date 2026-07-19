package registry_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sohamghodake/ironwork/internal/registry"
)

func newTestRegistry(now *time.Time) *registry.Registry {
	r := registry.New(5*time.Second, 8*time.Second)
	r.SetClock(func() time.Time { return *now })
	return r
}

func TestAvailableOrdersByHeadroom(t *testing.T) {
	now := time.Now()
	r := newTestRegistry(&now)

	r.Record("worker-1", 4, 3) // 1 free
	r.Record("worker-2", 4, 1) // 3 free
	r.Record("worker-3", 2, 2) // full

	assert.Equal(t, []string{"worker-2", "worker-1"}, r.Available())
}

func TestStaleWorkersLeaveThePool(t *testing.T) {
	now := time.Now()
	r := newTestRegistry(&now)
	r.Record("worker-1", 4, 0)

	now = now.Add(6 * time.Second) // past staleTTL, before deadTTL
	assert.Empty(t, r.Available(), "stale workers take no new placements")
	assert.Empty(t, r.Dead(), "but are not yet presumed crashed")

	snap := r.Snapshot()
	assert.True(t, snap[0].Alive)

	now = now.Add(3 * time.Second) // past deadTTL
	assert.Equal(t, []string{"worker-1"}, r.Dead())
	assert.False(t, r.Snapshot()[0].Alive)
}

func TestHeartbeatRevivesWorker(t *testing.T) {
	now := time.Now()
	r := newTestRegistry(&now)
	r.Record("worker-1", 4, 0)

	now = now.Add(10 * time.Second)
	assert.Equal(t, []string{"worker-1"}, r.Dead())

	r.Record("worker-1", 4, 0) // heartbeat resumes
	assert.Empty(t, r.Dead())
	assert.Equal(t, []string{"worker-1"}, r.Available())
}

func TestForgetDropsWorker(t *testing.T) {
	now := time.Now()
	r := newTestRegistry(&now)
	r.Record("worker-1", 4, 0)

	r.Forget("worker-1")
	assert.Empty(t, r.Snapshot())
	assert.Empty(t, r.Dead())
}
