package eventbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMatch(t *testing.T) {
	tests := []struct {
		topic   string
		pattern string
		want    bool
	}{
		{"orders.created", "orders.created", true},
		{"orders.created", "orders.updated", false},
		{"orders.created", "orders.*", true},
		{"orders.created", "*.created", true},
		{"orders.created.eu", "orders.*", false}, // * is exactly one segment
		{"orders.created.eu", "orders.>", true},
		{"orders.created", "orders.>", true}, // > is one or more
		{"orders", "orders.>", false},        // > never matches zero segments
		{"orders.created", ">", true},
		{"orders.created.eu", ">", true},
		{"orders", ">", true},
		{"a.b.c", "a.*.c", true},
		{"a.b.d", "a.*.c", false},
		{"a.b.c", "*.*.*", true},
		{"a.b", "*.*.*", false},
	}
	for _, tc := range tests {
		t.Run(tc.topic+"~"+tc.pattern, func(t *testing.T) {
			if got := match(tc.topic, tc.pattern); got != tc.want {
				t.Fatalf("match(%q, %q) = %v, want %v", tc.topic, tc.pattern, got, tc.want)
			}
		})
	}
}

func TestSubscribeRejectsInvalidPattern(t *testing.T) {
	b := New()
	defer b.Close()

	h := func(context.Context, Event) error { return nil }
	invalid := []string{"", "a..b", "a.", "a.>.b", "a.b>", "or*ders.x", ">" /* legal */}

	for _, pattern := range invalid {
		if pattern == ">" {
			if _, err := b.Subscribe(pattern, h); err != nil {
				t.Fatalf("Subscribe(%q) = %v, want nil: bare > is legal", pattern, err)
			}
			continue
		}
		if _, err := b.Subscribe(pattern, h); !errors.Is(err, ErrInvalidPattern) {
			t.Fatalf("Subscribe(%q) = %v, want ErrInvalidPattern", pattern, err)
		}
	}

	if _, err := b.Subscribe("ok.topic", nil); !errors.Is(err, ErrInvalidPattern) {
		t.Fatalf("Subscribe with nil handler = %v, want ErrInvalidPattern", err)
	}
}

func TestPublishRoutesToMatchingSubscribersOnly(t *testing.T) {
	b := New()
	defer b.Close()

	var orders, all atomic.Int64

	subOrders, err := b.Subscribe("orders.>", func(context.Context, Event) error {
		orders.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subOrders.Close()

	subAll, err := b.Subscribe(">", func(context.Context, Event) error {
		all.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subAll.Close()

	if err := b.Publish(context.Background(), Event{Topic: "orders.created"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := b.Publish(context.Background(), Event{Topic: "users.deleted"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	waitFor(t, "both subscriptions to settle", func() bool {
		return orders.Load() == 1 && all.Load() == 2
	})
}

func TestPublishFillsEventTime(t *testing.T) {
	fixed := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	b := New(WithClock(func() time.Time { return fixed }), WithBuffer(4))
	defer b.Close()

	got := make(chan Event, 1)
	if _, err := b.Subscribe("t", func(_ context.Context, e Event) error {
		got <- e
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := b.Publish(context.Background(), Event{Topic: "t"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case e := <-got:
		if !e.At.Equal(fixed) {
			t.Fatalf("At = %v, want %v", e.At, fixed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event was not delivered")
	}
}

func TestPublishSyncWaitsForHandlers(t *testing.T) {
	b := New()
	defer b.Close()

	var completed atomic.Int64
	for range 3 {
		if _, err := b.Subscribe("sync.topic", func(context.Context, Event) error {
			time.Sleep(10 * time.Millisecond)
			completed.Add(1)
			return nil
		}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}

	if err := b.PublishSync(context.Background(), Event{Topic: "sync.topic"}); err != nil {
		t.Fatalf("PublishSync: %v", err)
	}
	if got := completed.Load(); got != 3 {
		t.Fatalf("completed = %d, want 3: PublishSync must wait for every handler", got)
	}
}

func TestPublishSyncAggregatesErrors(t *testing.T) {
	b := New()
	defer b.Close()

	sentinel1 := errors.New("first failure")
	sentinel2 := errors.New("second failure")

	for _, err := range []error{sentinel1, sentinel2, nil} {
		if _, subErr := b.Subscribe("err.topic", func(context.Context, Event) error {
			return err
		}); subErr != nil {
			t.Fatalf("Subscribe: %v", subErr)
		}
	}

	err := b.PublishSync(context.Background(), Event{Topic: "err.topic"})
	if !errors.Is(err, sentinel1) || !errors.Is(err, sentinel2) {
		t.Fatalf("PublishSync error %v must join both handler errors", err)
	}
}

func TestDropNewestCountsDropsAndNeverBlocksPublisher(t *testing.T) {
	block := make(chan struct{})
	b := New(WithBuffer(2))
	defer b.Close()

	sub, err := b.Subscribe("slow", func(context.Context, Event) error {
		<-block // hold the single worker so the buffer fills
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Far more events than the buffer can hold. None of these may block.
	for i := range 50 {
		done := make(chan error, 1)
		go func() { done <- b.Publish(ctx, Event{Topic: "slow"}) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Publish %d: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Publish %d blocked with DropNewest policy", i)
		}
	}

	if got := sub.Dropped(); got == 0 {
		t.Fatal("Dropped() = 0, want > 0 when 50 events hit a 2-slot buffer")
	}
	close(block)
}

func TestBlockPolicyAppliesBackpressureThenUnblocksOnClose(t *testing.T) {
	block := make(chan struct{})
	b := New(WithBuffer(1))
	defer b.Close()

	sub, err := b.Subscribe("bp", func(context.Context, Event) error {
		<-block
		return nil
	}, WithDropPolicy(Block))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := b.Publish(ctx, Event{Topic: "bp"}); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	if err := b.Publish(ctx, Event{Topic: "bp"}); err != nil {
		t.Fatalf("second Publish: %v", err)
	}

	// The buffer is full and the worker is parked, so this must block.
	blocked := make(chan error, 1)
	go func() { blocked <- b.Publish(ctx, Event{Topic: "bp"}) }()

	select {
	case <-blocked:
		t.Fatal("third Publish returned immediately, want backpressure")
	case <-time.After(50 * time.Millisecond):
	}

	// Closing the subscription must release the parked publisher rather than
	// deadlocking it forever.
	close(block)
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("Publish stayed blocked after the subscription closed")
	}
}

func TestHandlerPanicIsIsolated(t *testing.T) {
	var reported atomic.Int64
	b := New(WithErrorHandler(func(Event, error) { reported.Add(1) }))
	defer b.Close()

	var okCount atomic.Int64
	sub, err := b.Subscribe("panicky", func(_ context.Context, e Event) error {
		if e.Payload == "boom" {
			panic("handler exploded")
		}
		okCount.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	ctx := context.Background()
	if err := b.Publish(ctx, Event{Topic: "panicky", Payload: "boom"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := b.Publish(ctx, Event{Topic: "panicky", Payload: "fine"}); err != nil {
		t.Fatalf("Publish after panic: %v", err)
	}

	waitFor(t, "panic to be reported and the later event to be handled", func() bool {
		return reported.Load() == 1 && okCount.Load() == 1
	})
}

func TestPublishAfterCloseReturnsErrBusClosed(t *testing.T) {
	b := New()
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if err := b.Publish(context.Background(), Event{Topic: "any"}); !errors.Is(err, ErrBusClosed) {
		t.Fatalf("Publish after Close = %v, want ErrBusClosed", err)
	}
	if err := b.PublishSync(context.Background(), Event{Topic: "any"}); !errors.Is(err, ErrBusClosed) {
		t.Fatalf("PublishSync after Close = %v, want ErrBusClosed", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("third Close: %v", err)
	}
}

func TestCloseWaitsForInFlightHandlers(t *testing.T) {
	b := New()

	var finished atomic.Bool
	sub, err := b.Subscribe("drain", func(context.Context, Event) error {
		time.Sleep(100 * time.Millisecond)
		finished.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_ = sub

	if err := b.Publish(context.Background(), Event{Topic: "drain"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !finished.Load() {
		t.Fatal("Close returned before the in-flight handler finished")
	}
}

func TestSubscriptionCloseIsIdempotent(t *testing.T) {
	b := New()
	defer b.Close()

	sub, err := b.Subscribe("x", func(context.Context, Event) error { return nil })
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	for range 3 {
		if err := sub.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

// TestConcurrentPublishSubscribeClose is the race-detector workhorse. It hammers
// the bus from many goroutines while subscriptions come and go, which is exactly
// the pattern that exposes a send-on-closed-channel bug.
func TestConcurrentPublishSubscribeClose(t *testing.T) {
	b := New(WithBuffer(4), WithWorkers(2))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var published atomic.Int64
	var wg sync.WaitGroup

	// Publishers.
	for p := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				if ctx.Err() != nil {
					return
				}
				topic := fmt.Sprintf("svc%d.topic%d", p, i%3)
				if err := b.Publish(ctx, Event{Topic: topic}); err != nil {
					return // bus closed: fine
				}
				published.Add(1)
			}
		}()
	}

	// A churning population of short-lived subscribers.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				sub, err := b.Subscribe("svc0.>", func(context.Context, Event) error { return nil })
				if err != nil {
					return
				}
				time.Sleep(5 * time.Millisecond)
				_ = sub.Close()
			}
		}()
	}

	// Concurrent PublishSync callers exercise a separate code path.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			_ = b.PublishSync(ctx, Event{Topic: "svc1.topic1"})
		}
	}()

	time.Sleep(300 * time.Millisecond)
	cancel()
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()

	if published.Load() == 0 {
		t.Fatal("no events were published")
	}
}

// waitFor polls cond until it holds or the deadline passes, with a generous
// margin so the test does not flake on a loaded machine.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
