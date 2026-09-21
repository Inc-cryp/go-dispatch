package eventbus

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestNoGoroutineLeakAfterClose verifies that a bus with workers and subscribers
// releases every goroutine when it is closed. The bus spawns one dispatcher per
// subscription plus a pool of workers, so a missed exit shows up here.
func TestNoGoroutineLeakAfterClose(t *testing.T) {
	before := runtime.NumGoroutine()

	for range 6 {
		bus := New(
			WithBuffer(16),
			WithWorkers(4),
			WithErrorHandler(func(Event, error) {}),
		)

		var subs []*Subscription
		for i := range 6 {
			s, err := bus.Subscribe("job.*", func(context.Context, Event) error { return nil },
				WithSubBuffer(8), WithSubWorkers(2))
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			_ = i
			subs = append(subs, s)
		}
		// One blocking subscriber: Close must unblock it rather than hang.
		blocking, err := bus.Subscribe(">", func(context.Context, Event) error { return nil },
			WithSubBuffer(1), WithDropPolicy(Block))
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		subs = append(subs, blocking)

		for i := range 50 {
			_ = bus.Publish(context.Background(), Event{Topic: "job.created", Payload: i})
		}
		if err := bus.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		for _, s := range subs {
			_ = s.Close()
		}
	}

	waitGoroutines(t, before, 4)
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
