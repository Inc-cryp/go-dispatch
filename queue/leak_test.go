package queue

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestNoGoroutineLeakAfterClose is the leak assertion for the whole package.
// Queue owns a scheduler goroutine and a fan-out goroutine for every
// subscription, so Close must account for all of them.
//
// The check is best-effort by nature: the runtime may be running unrelated
// goroutines, so the comparison is a tolerance rather than an equality, and the
// budget is retried to absorb transient goroutines from the test framework.
func TestNoGoroutineLeakAfterClose(t *testing.T) {
	before := runtime.NumGoroutine()

	const subscribers = 8
	for range subscribers {
		q := New(WithVisibility(50 * time.Millisecond))

		subs := make([]*Subscription, 0, 4)
		subs = append(subs,
			q.Subscribe(TopicEnqueued),
			q.Subscribe(TopicDequeued, TopicDone),
			q.Subscribe(TopicFailed, TopicRetried, TopicRequeued),
		)

		ctx, cancel := context.WithCancel(context.Background())
		for i := range 20 {
			if err := q.Enqueue(Entry{ID: idFor(i), Payload: i}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
		}

		// Consume a few jobs so the reservation and ack paths are exercised too.
		for range 5 {
			d, err := q.Dequeue(ctx)
			if err != nil {
				t.Fatalf("Dequeue: %v", err)
			}
			if err := q.Ack(d); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}

		cancel()
		if err := q.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		for _, s := range subs {
			_ = s.Close()
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
	t.Fatalf("goroutines grew from %d to %d after Close; stacks:\n%s", before, after, buf)
}
