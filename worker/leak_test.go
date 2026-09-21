package worker

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/Inc-cryp/go-dispatch/queue"
)

// TestNoGoroutineLeakAfterShutdown verifies the pool releases its dispatcher and
// every worker goroutine. A pool that leaks workers is worse than a slow one:
// the leak is invisible until the process is under load.
func TestNoGoroutineLeakAfterShutdown(t *testing.T) {
	before := runtime.NumGoroutine()

	for range 6 {
		q := queue.New(queue.WithVisibility(100 * time.Millisecond))

		pool := New(q,
			WithWorkers(8),
			WithHandler(func(context.Context, queue.Delivery) error { return nil }),
		)
		ctx, cancel := context.WithCancel(context.Background())
		if err := pool.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		for i := range 100 {
			if err := q.Enqueue(queue.Entry{ID: idFor(i), Payload: i}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
		}

		// Drain before shutting down so the test measures the steady-state
		// goroutine count rather than a pool busy tearing down.
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) && q.Stats().Done < 100 {
			time.Sleep(5 * time.Millisecond)
		}

		if err := pool.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		cancel()
		if err := q.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	waitGoroutines(t, before, 4)
}

func idFor(i int) string {
	return "job-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
}

// waitGoroutines retries the comparison so a goroutine that is merely on its way
// out does not fail the test.
func waitGoroutines(t *testing.T, before, slack int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var after int
	for time.Now().Before(deadline) {
		runtime.GC()
		after = runtime.NumGoroutine()
		if after <= before+slack {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	buf := make([]byte, 1<<16)
	buf = buf[:runtime.Stack(buf, true)]
	t.Fatalf("goroutines grew from %d to %d after Shutdown; stacks:\n%s", before, after, buf)
}
