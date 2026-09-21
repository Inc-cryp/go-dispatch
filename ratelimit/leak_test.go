package ratelimit

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestNoGoroutineLeakAfterUse is the structural assertion behind this package's
// central design claim: it starts no goroutines at all. Refills happen lazily on
// each call, so there is no ticker, no janitor, and nothing to leak.
//
// If someone later "optimises" this into a background refill loop, this test
// fails and forces that decision to be deliberate.
func TestNoGoroutineLeakAfterUse(t *testing.T) {
	before := runtime.NumGoroutine()

	clk := newFakeClock()
	for range 20 {
		tb := NewTokenBucket(100, 10, WithClock(clk.Now))
		fw := NewFixedWindow(100, time.Second, WithClock(clk.Now))
		keyed := NewKeyed(func() Limiter { return NewTokenBucket(10, 5, WithClock(clk.Now)) }, 8)
		m := Multi(tb, fw, keyed.Get("k"))

		for range 50 {
			_ = tb.Allow()
			_ = tb.Reserve()
			_ = tb.Tokens()
			_ = fw.Allow()
			_ = m.Allow()
			_ = m.Reserve()
			_ = keyed.Get("k")
			_ = keyed.Len()
		}
		clk.Advance(time.Second)
	}

	// A limiter with a cancelled context must return rather than spin.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tb := NewTokenBucket(1, 1)
	if err := tb.Wait(ctx); err == nil {
		t.Fatal("Wait returned nil for a cancelled context")
	}

	waitGoroutines(t, before, 2)
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
	t.Fatalf("goroutines grew from %d to %d; stacks:\n%s", before, after, buf)
}
