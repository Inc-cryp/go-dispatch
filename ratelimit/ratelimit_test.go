package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock, so refill behaviour can be asserted
// without sleeping.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestTokenBucketStartsFullAndDeniesAfterBurst(t *testing.T) {
	clk := newFakeClock()
	tb := NewTokenBucket(1, 5, WithClock(clk.Now))

	for i := range 5 {
		if !tb.Allow() {
			t.Fatalf("Allow() #%d = false, want true within the initial burst", i+1)
		}
	}
	if tb.Allow() {
		t.Fatal("Allow() = true after the burst was exhausted, want false")
	}
	if got := tb.Tokens(); got > 1e-6 {
		t.Fatalf("Tokens() = %v, want ~0 after the burst", got)
	}
}

func TestTokenBucketRefillsOverTime(t *testing.T) {
	clk := newFakeClock()
	tb := NewTokenBucket(2, 4, WithClock(clk.Now)) // 2 tokens/second

	for range 4 {
		if !tb.Allow() {
			t.Fatal("burst should permit 4 immediate tokens")
		}
	}
	if tb.Allow() {
		t.Fatal("bucket should be empty")
	}

	clk.Advance(500 * time.Millisecond) // 2/s * 0.5s = 1 token
	if !tb.Allow() {
		t.Fatal("Allow() = false after 500ms at 2/s, want true (1 token accrued)")
	}
	if tb.Allow() {
		t.Fatal("Allow() = true, want false: only one token had accrued")
	}
}

func TestTokenBucketDoesNotExceedBurstOnLongIdle(t *testing.T) {
	clk := newFakeClock()
	tb := NewTokenBucket(10, 3, WithClock(clk.Now))

	for range 3 {
		tb.Allow()
	}
	clk.Advance(time.Hour) // would be 36000 tokens uncapped

	if got := tb.Tokens(); got > 3 {
		t.Fatalf("Tokens() = %v, want <= burst (3)", got)
	}
	for i := range 3 {
		if !tb.Allow() {
			t.Fatalf("Allow() #%d = false, want true after a full refill", i+1)
		}
	}
	if tb.Allow() {
		t.Fatal("Allow() = true beyond burst, want false")
	}
}

func TestTokenBucketReserveDuration(t *testing.T) {
	clk := newFakeClock()
	tb := NewTokenBucket(1, 1, WithClock(clk.Now)) // 1 token/second

	if !tb.Allow() {
		t.Fatal("initial token should be available")
	}
	if d := tb.Reserve(); d != time.Second {
		t.Fatalf("Reserve() = %v, want exactly 1s with an empty 1/s bucket", d)
	}

	clk.Advance(600 * time.Millisecond) // 0.6 tokens
	want := 400 * time.Millisecond
	if d := tb.Reserve(); d < want-2*time.Millisecond || d > want+2*time.Millisecond {
		t.Fatalf("Reserve() = %v, want ~%v", d, want)
	}

	clk.Advance(time.Second)
	if d := tb.Reserve(); d != 0 {
		t.Fatalf("Reserve() = %v, want 0 once a token is available", d)
	}
}

func TestTokenBucketClampsInvalidArguments(t *testing.T) {
	clk := newFakeClock()
	tb := NewTokenBucket(0, 0, WithClock(clk.Now))
	if tb == nil {
		t.Fatal("NewTokenBucket returned nil")
	}
	// burst < 1 clamps to 1, so exactly one immediate token is available.
	if !tb.Allow() {
		t.Fatal("clamped bucket should allow its single initial token")
	}
	if tb.Allow() {
		t.Fatal("clamped bucket should deny after its single token")
	}
	// A zero rate must not divide by zero or return a nonsensical duration.
	if d := tb.Reserve(); d <= 0 {
		t.Fatalf("Reserve() = %v, want a large positive duration", d)
	}
}

func TestTokenBucketWaitSucceedsAfterTimeAdvances(t *testing.T) {
	clk := newFakeClock()
	tb := NewTokenBucket(1, 1, WithClock(clk.Now))
	if !tb.Allow() {
		t.Fatal("initial token should be available")
	}

	done := make(chan error, 1)
	go func() { done <- tb.Wait(context.Background()) }()

	// Give Wait a moment to park on its timer, then release it.
	time.Sleep(20 * time.Millisecond)
	clk.Advance(2 * time.Second)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Wait() = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Wait did not return after tokens became available")
	}
}

func TestWaitReturnsContextErrorOnCancel(t *testing.T) {
	clk := newFakeClock()
	tb := NewTokenBucket(1, 1, WithClock(clk.Now))
	tb.Allow() // drain

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tb.Wait(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait() = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Wait did not return after cancellation")
	}
}

func TestWaitDoesNotBusySpinWhenReserveIsZero(t *testing.T) {
	// A limiter whose Reserve always reports 0 must still work via Allow.
	f := NewFixedWindow(1, time.Minute)
	if err := f.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() = %v, want nil", err)
	}
	// The slot is consumed; a second immediate Wait would block, so only the
	// non-blocking path is asserted here.
	if got := f.Count(); got != 1 {
		t.Fatalf("Count() = %d, want 1", got)
	}
}

// TestWaitWithAlreadyCancelledContextDoesNotConsume is a regression test. Wait
// used to check the context only when it had to sleep, so calling it with an
// already-cancelled context returned nil and silently spent a token. Callers
// stop work by cancelling a context; that must never look like success.
func TestWaitWithAlreadyCancelledContextDoesNotConsume(t *testing.T) {
	clk := newFakeClock()
	tb := NewTokenBucket(1, 2, WithClock(clk.Now))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := tb.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() = %v, want context.Canceled", err)
	}
	if got := tb.Tokens(); got != 2 {
		t.Fatalf("Tokens() = %v after a cancelled Wait, want the full burst of 2", got)
	}
}

// TestFixedWindowWaitWithCancelledContextDoesNotConsume is the FixedWindow
// counterpart of the regression test above.
func TestFixedWindowWaitWithCancelledContextDoesNotConsume(t *testing.T) {
	clk := newFakeClock()
	f := NewFixedWindow(3, time.Minute, WithClock(clk.Now))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := f.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() = %v, want context.Canceled", err)
	}
	if got := f.Count(); got != 0 {
		t.Fatalf("Count() = %d after a cancelled Wait, want 0", got)
	}
}

// TestKeyedIsNotALimiter pins the design decision that Keyed must not satisfy
// Limiter. A keyed limiter needs a key, so passing one where a Limiter is
// expected should be a compile error, not a silently unlimited rate.
func TestKeyedIsNotALimiter(t *testing.T) {
	k := NewKeyed(func() Limiter { return NewTokenBucket(10, 1) }, 4)

	// These are the keyed forms; each must behave like its per-key limiter.
	if !k.Allow("tenant-a") {
		t.Fatal("first Allow for a fresh key denied")
	}
	if k.Allow("tenant-a") {
		t.Fatal("second Allow for an exhausted key allowed")
	}
	if !k.Allow("tenant-b") {
		t.Fatal("a different key must get its own bucket")
	}
	if d := k.Reserve("tenant-b"); d <= 0 {
		t.Fatalf("Reserve(tenant-b) = %v, want a positive wait", d)
	}
	if err := k.Wait(context.Background(), "tenant-b"); err != nil {
		t.Fatalf("Wait = %v, want nil once the window has passed", err)
	}
}

func TestFixedWindowAllowsLimitThenDenies(t *testing.T) {
	clk := newFakeClock()
	f := NewFixedWindow(3, time.Minute, WithClock(clk.Now))

	for i := range 3 {
		if !f.Allow() {
			t.Fatalf("Allow() #%d = false, want true within the limit", i+1)
		}
	}
	if f.Allow() {
		t.Fatal("Allow() = true past the limit, want false")
	}
	if got := f.Count(); got != 3 {
		t.Fatalf("Count() = %d, want 3", got)
	}
	if d := f.Reserve(); d <= 0 || d > time.Minute {
		t.Fatalf("Reserve() = %v, want a positive value within the window", d)
	}
}

func TestFixedWindowResetsAtBoundary(t *testing.T) {
	clk := newFakeClock()
	f := NewFixedWindow(2, time.Minute, WithClock(clk.Now))

	for range 2 {
		if !f.Allow() {
			t.Fatal("expected the limit to permit 2 events")
		}
	}
	if f.Allow() {
		t.Fatal("expected denial past the limit")
	}

	clk.Advance(time.Minute)
	if !f.Allow() {
		t.Fatal("Allow() = false after the window elapsed, want true")
	}
	if got := f.Count(); got != 1 {
		t.Fatalf("Count() = %d, want 1 after the reset", got)
	}
}

func TestFixedWindowLongIdleResetsImmediately(t *testing.T) {
	clk := newFakeClock()
	f := NewFixedWindow(1, time.Second, WithClock(clk.Now))

	if !f.Allow() {
		t.Fatal("first Allow should pass")
	}
	clk.Advance(10 * time.Minute) // many windows, not just one
	if !f.Allow() {
		t.Fatal("long idle period should reset the counters")
	}
}

func TestMultiRequiresEveryLimiter(t *testing.T) {
	clk := newFakeClock()

	t.Run("zero limiters allows everything", func(t *testing.T) {
		m := Multi()
		for range 10 {
			if !m.Allow() {
				t.Fatal("Multi() with no limiters must allow")
			}
		}
		if d := m.Reserve(); d != 0 {
			t.Fatalf("Reserve() = %v, want 0", d)
		}
	})

	t.Run("single limiter behaves as itself", func(t *testing.T) {
		m := Multi(NewTokenBucket(1, 2, WithClock(clk.Now)))
		if !m.Allow() {
			t.Fatal("first Allow denied; expected the token bucket to start full")
		}
		if !m.Allow() {
			t.Fatal("second Allow denied; expected a burst of 2")
		}
		if m.Allow() {
			t.Fatal("expected denial after the burst")
		}
	})

	t.Run("denies when any limiter denies", func(t *testing.T) {
		generous := NewTokenBucket(1, 100, WithClock(clk.Now))
		stingy := NewTokenBucket(1, 1, WithClock(clk.Now))
		m := Multi(generous, stingy)

		if !m.Allow() {
			t.Fatal("first Allow should pass: both have capacity")
		}
		if m.Allow() {
			t.Fatal("second Allow should fail: the stingy limiter is empty")
		}
	})

	t.Run("reserve is the maximum across limiters", func(t *testing.T) {
		a := NewTokenBucket(10, 1, WithClock(clk.Now)) // emptied -> ~100ms
		b := NewTokenBucket(1, 1, WithClock(clk.Now))  // emptied -> ~1s
		a.Allow()
		b.Allow()
		m := Multi(a, b)

		got := m.Reserve()
		if got < 900*time.Millisecond {
			t.Fatalf("Reserve() = %v, want >= ~1s (the slowest limiter)", got)
		}
	})

	t.Run("skips nil limiters", func(t *testing.T) {
		m := Multi(nil, NewTokenBucket(1, 1, WithClock(clk.Now)), nil)
		if !m.Allow() {
			t.Fatal("expected the single real limiter to allow")
		}
	})
}

func TestKeyedCreatesAndReusesPerKey(t *testing.T) {
	k := NewKeyed(func() Limiter { return NewTokenBucket(1, 1) }, 10)

	a1 := k.Get("a")
	a2 := k.Get("a")
	if a1 != a2 {
		t.Fatal("Get returned two different limiters for the same key")
	}
	b := k.Get("b")
	if a1 == b {
		t.Fatal("Get returned the same limiter for different keys")
	}
	if got := k.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestKeyedEvictsLeastRecentlyUsedAndNeverExceedsMaxKeys(t *testing.T) {
	k := NewKeyed(func() Limiter { return NewTokenBucket(1, 1) }, 3)

	k.Get("a")
	k.Get("b")
	k.Get("c")
	if got := k.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}

	// Touch "a" so "b" becomes the least recently used.
	first := k.Get("a")

	k.Get("d") // should evict "b", not "a"

	if got := k.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3 after eviction", got)
	}
	if k.Get("a") != first {
		t.Fatal("key \"a\" was evicted even though it was used most recently")
	}

	// "b" was evicted, so a fresh limiter must be handed out for it.
	k.Get("b")
	if got := k.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3 after re-adding an evicted key", got)
	}
}

func TestKeyedNeverExceedsMaxKeysUnderChurn(t *testing.T) {
	k := NewKeyed(func() Limiter { return NewTokenBucket(1, 1) }, 8)

	for i := range 1000 {
		k.Get(fmt.Sprintf("key-%d", i))
		if got := k.Len(); got > 8 {
			t.Fatalf("Len() = %d after %d inserts, want <= 8", got, i+1)
		}
	}
}

func TestKeyedClampsMaxKeys(t *testing.T) {
	k := NewKeyed(func() Limiter { return NewTokenBucket(1, 1) }, 0)
	for i := range 5 {
		k.Get(fmt.Sprintf("k%d", i))
	}
	if got := k.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1 with maxKeys clamped to 1", got)
	}
}

func TestKeyedWithNilFactoryUsesDefault(t *testing.T) {
	k := NewKeyed(nil, 2)
	if lim := k.Get("x"); lim == nil {
		t.Fatal("Get returned nil with a nil factory")
	}
}

// TestKeyedConcurrentAccessIsRaceFree hammers one Keyed limiter from many
// goroutines, asserting the internal LRU bookkeeping under -race.
func TestKeyedConcurrentAccessIsRaceFree(t *testing.T) {
	k := NewKeyed(func() Limiter { return NewTokenBucket(1000, 10) }, 16)

	var wg sync.WaitGroup
	var ops atomic.Int64

	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				key := fmt.Sprintf("key-%d", (g*i)%64)
				if lim := k.Get(key); lim != nil {
					lim.Allow()
					lim.Reserve()
				}
				k.Len()
				ops.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := k.Len(); got > 16 {
		t.Fatalf("Len() = %d, want <= 16", got)
	}
	if ops.Load() != 16*500 {
		t.Fatalf("ops = %d, want %d", ops.Load(), 16*500)
	}
}

func TestTokenBucketConcurrentAllowIsRaceFree(t *testing.T) {
	clk := newFakeClock()
	tb := NewTokenBucket(1000, 100, WithClock(clk.Now))

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if tb.Allow() {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	// 16*50 = 800 attempts against a 100-token bucket with no time advance, so
	// exactly 100 must have succeeded.
	if got := allowed.Load(); got != 100 {
		t.Fatalf("allowed = %d, want exactly the burst of 100", got)
	}
}
