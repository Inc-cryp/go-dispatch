package worker

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/abdillahfazri/dispatch/queue"
	"github.com/abdillahfazri/dispatch/ratelimit"
)

// benchSink is a Sink backed by a channel, so the pool benchmarks measure the
// pool's own scheduling rather than the real queue's heap and locking. The
// queue's cost is already measured in its own package.
type benchSink struct {
	ch   chan queue.Delivery
	done chan struct{}
	acks chan struct{}

	mu    sync.Mutex
	nacks int
}

func newBenchSink(capacity int) *benchSink {
	return &benchSink{
		ch:   make(chan queue.Delivery, capacity),
		done: make(chan struct{}),
		acks: make(chan struct{}, capacity),
	}
}

func (s *benchSink) Dequeue(ctx context.Context) (queue.Delivery, error) {
	select {
	case d := <-s.ch:
		return d, nil
	case <-s.done:
		return queue.Delivery{}, queue.ErrClosed
	case <-ctx.Done():
		return queue.Delivery{}, ctx.Err()
	}
}

func (s *benchSink) Ack(queue.Delivery) error {
	// A buffered signal per ack lets the benchmark wait for completion without
	// polling: each send is non-blocking because the buffer matches the
	// workload size.
	s.acks <- struct{}{}
	return nil
}

func (s *benchSink) Nack(queue.Delivery, error) error {
	s.mu.Lock()
	s.nacks++
	s.mu.Unlock()
	return nil
}

func (s *benchSink) Extend(queue.Delivery, time.Duration) error { return nil }

func (s *benchSink) close() { close(s.done) }

// waitForAcks blocks until the sink has seen n acks.
func (s *benchSink) waitForAcks(n int) {
	for range n {
		<-s.acks
	}
}

// fill pre-loads the sink with n deliveries before the timer starts, so the
// benchmark measures processing rather than the producer's ability to keep up.
func (s *benchSink) fill(n int) {
	for i := range n {
		s.ch <- queue.Delivery{
			Job:     queue.Entry{ID: "job-" + strconv.Itoa(i)},
			Attempt: 1,
		}
	}
}

// runPool is the shared setup/teardown for the pool benchmarks: start a pool,
// pre-fill the sink, run the timed section, then shut down.
func runPool(b *testing.B, workers int, limiter ratelimit.Limiter, onResult func(Result)) {
	b.Helper()

	sink := newBenchSink(b.N + 1)
	defer sink.close()

	opts := []Option{
		WithWorkers(workers),
		WithHandler(func(context.Context, queue.Delivery) error { return nil }),
	}
	if limiter != nil {
		opts = append(opts, WithLimiter(limiter))
	}
	if onResult != nil {
		opts = append(opts, WithOnResult(onResult))
	}
	pool := New(sink, opts...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := pool.Start(ctx); err != nil {
		b.Fatalf("Start: %v", err)
	}

	sink.fill(b.N)

	b.ReportAllocs()
	b.ResetTimer()
	sink.waitForAcks(b.N)
	b.StopTimer()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := pool.Shutdown(shutdownCtx); err != nil {
		b.Fatalf("Shutdown: %v", err)
	}
}

// BenchmarkPoolThroughput measures jobs per second through a pool with a no-op
// handler. This is the ceiling: a real handler only makes it slower, so the
// number isolates the pool's scheduling overhead.
func BenchmarkPoolThroughput(b *testing.B) {
	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			runPool(b, workers, nil, nil)
		})
	}
}

// BenchmarkPoolWithLimiter measures the cost of a rate limiter in the hot path.
// The limiter always allows, so what is measured is the per-job call and its
// mutex, not throttling.
func BenchmarkPoolWithLimiter(b *testing.B) {
	runPool(b, 8, alwaysAllow{}, nil)
}

// BenchmarkResultCallback measures the overhead of the OnResult observation hook
// against the same pool without it.
func BenchmarkResultCallback(b *testing.B) {
	b.Run("disabled", func(b *testing.B) {
		runPool(b, 8, nil, nil)
	})
	b.Run("enabled", func(b *testing.B) {
		runPool(b, 8, nil, func(Result) {})
	})
}

// BenchmarkPoolStartShutdown measures the lifecycle cost: standing up a pool and
// tearing it down with nothing in flight. Short-lived pools are common in tests
// and CLI tools, so the constant matters.
func BenchmarkPoolStartShutdown(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		sink := newBenchSink(1)
		pool := New(sink,
			WithWorkers(4),
			WithHandler(func(context.Context, queue.Delivery) error { return nil }),
		)
		ctx, cancel := context.WithCancel(context.Background())
		if err := pool.Start(ctx); err != nil {
			b.Fatalf("Start: %v", err)
		}
		if err := pool.Shutdown(context.Background()); err != nil {
			b.Fatalf("Shutdown: %v", err)
		}
		cancel()
		sink.close()
	}
}

// alwaysAllow is a Limiter that never throttles, so the limiter benchmarks
// measure call overhead rather than throttling.
type alwaysAllow struct{}

func (alwaysAllow) Allow() bool                    { return true }
func (alwaysAllow) Reserve() time.Duration         { return 0 }
func (alwaysAllow) Wait(ctx context.Context) error { return ctx.Err() }
