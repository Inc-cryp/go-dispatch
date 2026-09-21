package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// benchClock is a deterministic clock for benchmarks. With a zero step it is
// frozen, so time-dependent paths (refill, window rollover) cannot fire
// mid-measurement and make a benchmark non-reproducible. With a non-zero step it
// advances on every read, which is how the granted paths are measured without
// ever blocking.
type benchClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

func newBenchClock() *benchClock {
	return &benchClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

// newAdvancingBenchClock returns a clock that moves forward by step on every
// read, so a limiter always finds capacity and the fast path is what gets
// measured.
func newAdvancingBenchClock(step time.Duration) *benchClock {
	c := newBenchClock()
	c.step = step
	return c
}

func (c *benchClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.now
	c.now = c.now.Add(c.step)
	return t
}

// BenchmarkTokenBucketAllow measures the single-goroutine decision path with the
// bucket permanently exhausted, which is the steady state under load.
func BenchmarkTokenBucketAllow(b *testing.B) {
	clk := newBenchClock()
	tb := NewTokenBucket(1, 1, WithClock(clk.Now))
	tb.Allow() // drain so every measured call takes the denial path

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		tb.Allow()
	}
}

// BenchmarkTokenBucketAllowGranted measures the granted path. The clock advances
// on every read, so the bucket always holds a token and the denial path is never
// taken. It includes the refill arithmetic, which is the cost of the lazy design.
func BenchmarkTokenBucketAllowGranted(b *testing.B) {
	clk := newAdvancingBenchClock(time.Millisecond)
	tb := NewTokenBucket(1000, 1, WithClock(clk.Now))

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		tb.Allow()
	}
}

// BenchmarkTokenBucketAllowParallel measures contention: every goroutine shares
// one mutex, so this is the honest number for a hot shared limiter.
func BenchmarkTokenBucketAllowParallel(b *testing.B) {
	clk := newBenchClock()
	tb := NewTokenBucket(1e6, 1e6, WithClock(clk.Now))

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow()
		}
	})
}

// BenchmarkTokenBucketReserve measures the wait-time computation, which is the
// heavier of the two decision paths because it also updates the token count.
// Reserve never blocks, so a frozen clock is enough to keep it deterministic.
func BenchmarkTokenBucketReserve(b *testing.B) {
	clk := newBenchClock()
	tb := NewTokenBucket(1000, 1, WithClock(clk.Now))
	tb.Allow()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		tb.Reserve()
	}
}

// BenchmarkFixedWindowAllow measures the cheap limiter's decision path while
// exhausted.
func BenchmarkFixedWindowAllow(b *testing.B) {
	clk := newBenchClock()
	f := NewFixedWindow(1, time.Minute, WithClock(clk.Now))
	f.Allow()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		f.Allow()
	}
}

// BenchmarkFixedWindowAllowParallel measures contention on the cheap limiter.
func BenchmarkFixedWindowAllowParallel(b *testing.B) {
	clk := newBenchClock()
	f := NewFixedWindow(1<<30, time.Minute, WithClock(clk.Now))

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			f.Allow()
		}
	})
}

// BenchmarkMultiAllow measures the combined decision across a growing number of
// limiters. Multi consults each in turn and stops at the first denial, so the
// cost should be close to linear in the number of limiters that allow.
func BenchmarkMultiAllow(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("limiters=%d", n), func(b *testing.B) {
			clk := newBenchClock()
			ls := make([]Limiter, 0, n)
			for range n {
				ls = append(ls, NewTokenBucket(1e6, 1e6, WithClock(clk.Now)))
			}
			m := Multi(ls...)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				m.Allow()
			}
		})
	}
}

// BenchmarkKeyedGet measures the keyed lookup path: map hit plus an LRU move to
// the front, all under one mutex. It is the hot path for per-tenant limiting.
func BenchmarkKeyedGet(b *testing.B) {
	clk := newBenchClock()
	k := NewKeyed(func() Limiter { return NewTokenBucket(1e6, 1e6, WithClock(clk.Now)) }, 1024)
	k.Get("tenant") // warm the entry so the measured path is a cache hit

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		k.Get("tenant")
	}
}

// BenchmarkKeyedGetParallel measures contention on the keyed map, which is the
// realistic shape: many tenants, many goroutines, one lock.
func BenchmarkKeyedGetParallel(b *testing.B) {
	clk := newBenchClock()
	k := NewKeyed(func() Limiter { return NewTokenBucket(1e6, 1e6, WithClock(clk.Now)) }, 1024)
	for i := range 64 {
		k.Get(fmt.Sprintf("tenant-%d", i))
	}

	var seq atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Read-mostly: cycle through the warm keys so the LRU move is
			// exercised without growing the map.
			key := fmt.Sprintf("tenant-%d", seq.Add(1)%64)
			k.Get(key)
		}
	})
}

// BenchmarkKeyedEviction measures the steady state where every Get evicts the
// least-recently-used entry, which is the worst case for the LRU bookkeeping.
func BenchmarkKeyedEviction(b *testing.B) {
	clk := newBenchClock()
	const maxKeys = 16
	k := NewKeyed(func() Limiter { return NewTokenBucket(1e6, 1e6, WithClock(clk.Now)) }, maxKeys)

	// Name generation outside the measured loop: this benchmark is about the
	// eviction bookkeeping, not string formatting.
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("tenant-%d", i)
	}

	var seq atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		k.Get(keys[seq.Add(1)%1024])
	}
}

// BenchmarkWaitImmediate measures the fast path of Wait: a token is available,
// so it grants without ever starting a timer. The clock advances on every read,
// which is required rather than cosmetic: Wait blocks on a real timer when it
// cannot grant, so a frozen clock would hang the benchmark once the burst is
// exhausted instead of measuring anything.
func BenchmarkWaitImmediate(b *testing.B) {
	// A Refill of one token per step means every Wait finds capacity.
	clk := newAdvancingBenchClock(time.Millisecond)
	tb := NewTokenBucket(1e6, 1e6, WithClock(clk.Now))
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := tb.Wait(ctx); err != nil {
			b.Fatalf("Wait: %v", err)
		}
	}
}
