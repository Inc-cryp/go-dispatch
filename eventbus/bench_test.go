package eventbus

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

// BenchmarkPublish measures the enqueue side: topic matching plus one buffered
// channel send per matching subscription. The handler is trivial, so what is
// measured is the bus's own bookkeeping.
func BenchmarkPublish(b *testing.B) {
	bus := New(WithBuffer(1<<16), WithWorkers(4))
	defer bus.Close()

	s, err := bus.Subscribe("job.>", func(context.Context, Event) error { return nil },
		WithSubBuffer(1<<16), WithSubWorkers(1))
	if err != nil {
		b.Fatalf("Subscribe: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := bus.Publish(ctx, Event{Topic: "job.created", Payload: i}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

// BenchmarkPublishParallel measures contention on the publish path. Every
// publisher shares one bus, so this is the number that matters when many
// goroutines report events.
func BenchmarkPublishParallel(b *testing.B) {
	bus := New(WithBuffer(1<<16), WithWorkers(4))
	defer bus.Close()

	s, err := bus.Subscribe(">", func(context.Context, Event) error { return nil },
		WithSubBuffer(1<<16), WithSubWorkers(4))
	if err != nil {
		b.Fatalf("Subscribe: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bus.Publish(ctx, Event{Topic: "job.created", Payload: 1}); err != nil {
				b.Fatalf("Publish: %v", err)
			}
		}
	})
}

// BenchmarkPublishNoMatch isolates matching cost when no subscriber matches: the
// pattern must be tested and rejected for every registered subscription.
func BenchmarkPublishNoMatch(b *testing.B) {
	bus := New(WithBuffer(1 << 16))
	defer bus.Close()

	for _, pat := range []string{"other.>", "different.*", "unrelated.topic"} {
		s, err := bus.Subscribe(pat, func(context.Context, Event) error { return nil })
		if err != nil {
			b.Fatalf("Subscribe: %v", err)
		}
		defer s.Close()
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := bus.Publish(ctx, Event{Topic: "job.created", Payload: i}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

// BenchmarkPublishSync measures synchronous delivery, where PublishSync does not
// return until the handler has run. This is the latency a caller pays when it
// needs the handler to have completed.
func BenchmarkPublishSync(b *testing.B) {
	bus := New()
	defer bus.Close()

	s, err := bus.Subscribe("job.created", func(context.Context, Event) error { return nil })
	if err != nil {
		b.Fatalf("Subscribe: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := bus.PublishSync(ctx, Event{Topic: "job.created", Payload: i}); err != nil {
			b.Fatalf("PublishSync: %v", err)
		}
	}
}

// BenchmarkSubscribeFanout measures fan-out cost as the subscriber count grows.
// The handlers are trivial and the buffers are large, so the publisher never
// applies backpressure and the measurement is the matching-plus-send cost.
func BenchmarkSubscribeFanout(b *testing.B) {
	for _, subs := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("subscribers=%d", subs), func(b *testing.B) {
			bus := New(WithBuffer(1<<16), WithWorkers(1))
			defer bus.Close()

			for range subs {
				s, err := bus.Subscribe("job.>", func(context.Context, Event) error { return nil },
					WithSubBuffer(1<<16))
				if err != nil {
					b.Fatalf("Subscribe: %v", err)
				}
				defer s.Close()
			}

			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if err := bus.Publish(ctx, Event{Topic: "job.created", Payload: i}); err != nil {
					b.Fatalf("Publish: %v", err)
				}
			}
		})
	}
}

// BenchmarkHandlerThroughput measures end-to-end delivery: Publish an event and
// wait until every one has actually been handled. It is the only benchmark that
// includes handler dispatch, so it is the realistic ceiling for this bus.
func BenchmarkHandlerThroughput(b *testing.B) {
	// The buffer is sized to b.N so no event is ever dropped: the default policy
	// is DropNewest, and a single dropped event would leave the handler count
	// short of b.N and hang the wait below.
	bus := New(WithBuffer(b.N+1), WithWorkers(4))
	defer bus.Close()

	var handled atomic.Int64
	done := make(chan struct{})
	s, err := bus.Subscribe("job.created", func(context.Context, Event) error {
		if handled.Add(1) == int64(b.N) {
			close(done)
		}
		return nil
	}, WithSubBuffer(b.N+1), WithSubWorkers(4))
	if err != nil {
		b.Fatalf("Subscribe: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := bus.Publish(ctx, Event{Topic: "job.created", Payload: i}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
	// Wait for the asynchronous handlers: without this the benchmark would
	// measure only the enqueue side and report a flattering number.
	<-done
}

// BenchmarkMatchTopic isolates the pattern-matching cost for the common shapes:
// an exact topic, a single-segment wildcard, and a trailing multi-segment one.
func BenchmarkMatchTopic(b *testing.B) {
	patterns := []string{
		"job.created",
		"job.*",
		"job.>",
		">",
	}
	for _, pat := range patterns {
		b.Run(pat, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				match("job.created", pat)
			}
		})
	}
}

// BenchmarkPublishDropNewest measures the overflow path, where the subscriber's
// buffer is full and every event is dropped and counted. That path must stay
// cheap: a slow subscriber is not allowed to slow the publisher down.
func BenchmarkPublishDropNewest(b *testing.B) {
	// A handler parked on a channel keeps the buffer permanently full, so the
	// drop path is what gets measured. The channel is closed at cleanup so the
	// handler returns and Close can drain instead of hanging.
	release := make(chan struct{})
	bus := New(WithBuffer(1), WithWorkers(1))
	defer bus.Close()

	s, err := bus.Subscribe("job.>", func(context.Context, Event) error {
		<-release
		return nil
	}, WithSubBuffer(1), WithSubWorkers(1), WithDropPolicy(DropNewest))
	if err != nil {
		b.Fatalf("Subscribe: %v", err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := bus.Publish(ctx, Event{Topic: "job.created", Payload: i}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
	b.StopTimer()

	close(release)
	if err := s.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
}
