package queue

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// benchClock is a monotonic fake clock. Benchmarks need a clock that never
// moves so that no job's visibility timeout lapses mid-run; using time.Now
// would inject scheduler jitter into the numbers.
type benchClock struct {
	mu  sync.Mutex
	now time.Time
}

func newBenchClock() *benchClock {
	return &benchClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *benchClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// BenchmarkEnqueue measures the submission path in isolation: dedup insert,
// heap push, and event fan-out.
func BenchmarkEnqueue(b *testing.B) {
	q := New(WithClock(newBenchClock().Now))
	defer q.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := q.Enqueue(Entry{ID: fmt.Sprintf("job-%d", i), Payload: i}); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
	}
}

// BenchmarkEnqueueParallel measures contention on the enqueue lock. The body
// size is large enough that the heap does not resize during the measured loop.
func BenchmarkEnqueueParallel(b *testing.B) {
	q := New(WithClock(newBenchClock().Now), WithEventBuffer(0))
	b.ResetTimer()
	var seq atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// IDs must be unique across goroutines; a timestamp is not enough
			// because several goroutines can read the same nanosecond. A
			// counter makes duplicates impossible and keeps the benchmark on
			// the happy path.
			id := "job-" + strconv.FormatUint(seq.Add(1), 10)
			if err := q.Enqueue(Entry{ID: id}); err != nil {
				b.Fatalf("Enqueue: %v", err)
			}
		}
	})
}

// BenchmarkEnqueueDequeueAck measures the full happy path with a pre-filled
// queue: pop from the heap, hand out a reservation, and release it.
func BenchmarkEnqueueDequeueAck(b *testing.B) {
	q := New(WithClock(newBenchClock().Now))
	defer q.Close()

	for i := range b.N {
		if err := q.Enqueue(Entry{ID: fmt.Sprintf("job-%d", i), Payload: i}); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d, err := q.Dequeue(ctx)
		if err != nil {
			b.Fatalf("Dequeue: %v", err)
		}
		if err := q.Ack(d); err != nil {
			b.Fatalf("Ack: %v", err)
		}
	}
}

// BenchmarkDequeuePriority measures the cost of ordering. Entries are pushed
// with pseudo-random priorities so the heap has to actually sift rather than
// walking a single sorted chain.
func BenchmarkDequeuePriority(b *testing.B) {
	q := New(WithClock(newBenchClock().Now))
	defer q.Close()

	for i := range b.N {
		if err := q.Enqueue(Entry{
			ID:       fmt.Sprintf("job-%d", i),
			Priority: (i * 2654435761) % 1000,
		}); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d, err := q.Dequeue(ctx)
		if err != nil {
			b.Fatalf("Dequeue: %v", err)
		}
		if err := q.Ack(d); err != nil {
			b.Fatalf("Ack: %v", err)
		}
	}
}

// BenchmarkDequeueParallel measures consumer scalability, including the mutex a
// caller-injected clock adds to every call.
//
// The queue is prefilled with exactly b.N jobs. RunParallel splits b.N across
// the goroutines, and each iteration consumes exactly one job, so the workload
// is sized right. A fixed-size prefill would be drained by a large b.N and the
// next Dequeue would block forever on an empty queue.
func BenchmarkDequeueParallel(b *testing.B) {
	q := New(WithClock(newBenchClock().Now))
	defer q.Close()

	for i := range b.N {
		if err := q.Enqueue(Entry{ID: fmt.Sprintf("job-%d", i)}); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d, err := q.Dequeue(ctx)
			if err != nil {
				b.Fatalf("Dequeue: %v", err)
			}
			if err := q.Ack(d); err != nil {
				b.Fatalf("Ack: %v", err)
			}
		}
	})
}

// BenchmarkSubscribeFanout measures notification cost as subscribers grow. Each
// subscriber gets a buffered channel, and the enqueue path must not block on a
// slow one.
func BenchmarkSubscribeFanout(b *testing.B) {
	for _, subs := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("subscribers=%d", subs), func(b *testing.B) {
			q := New(WithClock(newBenchClock().Now), WithEventBuffer(1<<16))
			defer q.Close()

			drain := make(chan struct{})
			var wg sync.WaitGroup
			for range subs {
				s := q.Subscribe(TopicEnqueued, TopicDone)
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case _, ok := <-s.C():
							if !ok {
								return
							}
						case <-drain:
							return
						}
					}
				}()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if err := q.Enqueue(Entry{ID: fmt.Sprintf("job-%d", i)}); err != nil {
					b.Fatalf("Enqueue: %v", err)
				}
			}
			b.StopTimer()
			close(drain)
			wg.Wait()
		})
	}
}
