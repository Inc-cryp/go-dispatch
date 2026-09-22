// Package ratelimit provides the throttling primitives used by dispatch.
//
// Four limiters implement one small Limiter interface, so callers can compose
// and swap them freely:
//
//   - TokenBucket: smooth, burst-tolerant, refilled lazily from a clock.
//   - FixedWindow: a cheap counter with an honest boundary caveat.
//   - Multi: several limiters that must all agree.
//   - Keyed: one limiter per key, with bounded memory via LRU eviction.
//
// # No background goroutines
//
// Nothing in this package starts a goroutine. A TokenBucket refills lazily: each
// call computes how much time has passed since the previous call and adds the
// corresponding tokens. That keeps the cost proportional to actual use, avoids a
// timer per limiter, and makes every limiter trivially testable by injecting a
// clock through WithClock.
//
// # Concurrency
//
// Every limiter is safe for concurrent use. The composite limiters (Multi,
// Keyed) guard their internal state with a mutex; the leaf limiters use one too,
// because a token bucket's check-and-consume must be atomic.
package ratelimit

import (
	"container/list"
	"context"
	"math"
	"sync"
	"time"
)

// Limiter decides whether an event may proceed.
type Limiter interface {
	// Allow reports whether one event may proceed now. It never blocks and
	// consumes capacity when it returns true.
	Allow() bool
	// Reserve reports how long to wait before one event may proceed. Zero means
	// now. It does not consume capacity.
	Reserve() time.Duration
	// Wait blocks until one event may proceed or ctx is done. On cancellation it
	// returns ctx.Err().
	Wait(ctx context.Context) error
}

type config struct {
	now func() time.Time
}

// Option configures a limiter.
type Option func(*config)

// WithClock injects the time source. It exists so tests can advance time
// explicitly instead of sleeping. Defaults to time.Now.
func WithClock(now func() time.Time) Option {
	return func(c *config) {
		if now != nil {
			c.now = now
		}
	}
}

func newConfig(opts []Option) config {
	c := config{now: time.Now}
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// TokenBucket is a leaky-bucket-style limiter that allows a burst up to its
// capacity and then refills at a steady rate.
//
// It refills lazily on each call rather than from a ticker, so an idle bucket
// costs nothing. Construction arguments are clamped rather than rejected: a
// non-positive rate becomes minRate, and a burst below 1 becomes 1.
type TokenBucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64 // maximum tokens
	tokens float64
	last   time.Time
	now    func() time.Time
}

// minRate is the floor applied to a non-positive rate so that a misconfigured
// limiter degrades into a very slow limiter instead of a division by zero or an
// accidental "allow everything".
const minRate = 1e-9

// NewTokenBucket creates a bucket that refills at rate tokens per second and
// holds at most burst tokens. The bucket starts full.
func NewTokenBucket(rate float64, burst int, opts ...Option) *TokenBucket {
	c := newConfig(opts)
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		rate = minRate
	}
	if burst < 1 {
		burst = 1
	}
	return &TokenBucket{
		rate:   rate,
		burst:  float64(burst),
		tokens: float64(burst), // start full so the first burst is allowed
		last:   c.now(),
		now:    c.now,
	}
}

// refill adds the tokens accrued since the last call. Callers must hold t.mu.
func (t *TokenBucket) refill() {
	now := t.now()
	elapsed := now.Sub(t.last)
	if elapsed <= 0 {
		return
	}
	t.last = now
	t.tokens = math.Min(t.burst, t.tokens+elapsed.Seconds()*t.rate)
}

// Allow reports whether one token is available, consuming it when so.
func (t *TokenBucket) Allow() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refill()
	if t.tokens >= 1 {
		t.tokens--
		return true
	}
	return false
}

// Reserve reports how long until one token is available. It does not consume.
//
// The wait is never reported as zero while the bucket is short of a token. The
// float-to-duration conversion truncates toward zero, so at a high rate it would
// otherwise round a real wait down to 0s; a caller that reads "zero means now"
// (waitLoop does) would then retry Allow in a tight loop. The floor of one
// nanosecond keeps the contract honest at any rate. At minRate the value is
// 1e18 ns, well inside time.Duration's int64 range.
func (t *TokenBucket) Reserve() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refill()
	if t.tokens >= 1 {
		return 0
	}
	missing := 1 - t.tokens
	ns := missing / t.rate * float64(time.Second)
	if ns < 1 {
		return time.Nanosecond
	}
	return time.Duration(ns)
}

// Tokens reports the current token level, after accounting for elapsed time.
func (t *TokenBucket) Tokens() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refill()
	return t.tokens
}

// Wait blocks until one token is available or ctx is done. A context that is
// already cancelled makes Wait return ctx.Err() without consuming a token.
func (t *TokenBucket) Wait(ctx context.Context) error {
	return waitLoop(ctx, t)
}

// FixedWindow limits to a fixed number of events per window, resetting the
// counter on a boundary.
//
// Caveat: because the counter resets all at once, a caller can observe up to
// twice the configured limit across a boundary (for example, limit events at the
// end of one window and limit more immediately after the reset). Use TokenBucket
// when a smoother rate matters; FixedWindow is the cheap option when an
// approximate ceiling is enough.
type FixedWindow struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	count  int
	start  time.Time
	now    func() time.Time
}

// NewFixedWindow creates a limiter allowing limit events per window. Arguments
// are clamped: limit below 1 becomes 1, and a non-positive window becomes 1s.
func NewFixedWindow(limit int, window time.Duration, opts ...Option) *FixedWindow {
	c := newConfig(opts)
	if limit < 1 {
		limit = 1
	}
	if window <= 0 {
		window = time.Second
	}
	return &FixedWindow{
		limit:  limit,
		window: window,
		start:  c.now(),
		now:    c.now,
	}
}

// roll advances the window if the current one has elapsed. Callers must hold
// f.mu.
func (f *FixedWindow) roll() {
	now := f.now()
	if now.Sub(f.start) >= f.window {
		// Jump straight to the current window rather than stepping one window
		// at a time, so a long idle period does not require a loop.
		f.start = now
		f.count = 0
	}
}

// Allow reports whether the window has room, consuming one slot when so.
func (f *FixedWindow) Allow() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.roll()
	if f.count < f.limit {
		f.count++
		return true
	}
	return false
}

// Reserve reports how long until the window resets. It does not consume.
func (f *FixedWindow) Reserve() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.roll()
	if f.count < f.limit {
		return 0
	}
	remaining := f.window - f.now().Sub(f.start)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// Count reports how many events have been allowed in the current window.
func (f *FixedWindow) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.roll()
	return f.count
}

// Wait blocks until the window has room or ctx is done. A context that is
// already cancelled makes Wait return ctx.Err() without consuming a slot.
func (f *FixedWindow) Wait(ctx context.Context) error {
	return waitLoop(ctx, f)
}

// waitLoop implements the shared Wait behaviour: reserve, and if there is
// capacity take it, otherwise sleep for the reserved duration and retry. It
// never busy-spins, because a zero reserve goes straight to Allow.
//
// The context is checked before any token is consumed. Consuming first would
// make a cancelled Wait a silent success that still spent capacity, which is the
// opposite of what a caller passing a cancelled context expects.
func waitLoop(ctx context.Context, l Limiter) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		d := l.Reserve()
		if d <= 0 {
			if l.Allow() {
				// A context cancelled while we were taking the token wins: the
				// token is already spent, but the caller asked to stop, so
				// report the cancellation rather than a bogus success.
				if err := ctx.Err(); err != nil {
					return err
				}
				return nil
			}
			// Lost a race with another waiter; fall through and re-reserve
			// rather than spinning, since Reserve will now be positive.
			continue
		}

		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// multiLimiter requires every limiter to allow.
type multiLimiter struct {
	limiters []Limiter
}

// Multi combines limiters so that an event proceeds only when every one of them
// allows it. With no limiters it allows everything.
//
// Allow consults the limiters in order and stops at the first denial. Note that
// the limiters already consulted have consumed a token even though the overall
// call failed. That is intentional: it keeps the combined rate strict, at the
// cost of consuming some capacity from the faster limiters on a rejected event.
// Order the limiters cheapest-or-strictest first to minimise the waste.
func Multi(ls ...Limiter) Limiter {
	filtered := make([]Limiter, 0, len(ls))
	for _, l := range ls {
		if l != nil {
			filtered = append(filtered, l)
		}
	}
	return &multiLimiter{limiters: filtered}
}

// Allow reports whether every limiter allows, consuming from each in turn.
func (m *multiLimiter) Allow() bool {
	for _, l := range m.limiters {
		if !l.Allow() {
			return false
		}
	}
	return true
}

// Reserve reports the longest wait across all limiters. Every child must be
// satisfied, so the maximum — not the sum — is what the caller has to wait.
func (m *multiLimiter) Reserve() time.Duration {
	var longest time.Duration
	for _, l := range m.limiters {
		if d := l.Reserve(); d > longest {
			longest = d
		}
	}
	return longest
}

// Wait blocks until every limiter has capacity.
//
// It goes through waitLoop rather than waiting on each child in turn. Waiting
// sequentially would take a token from an early child before a later one has had
// any chance to refuse, so a composite that ends up failing would still have
// spent capacity it never delivered — and the same would happen when the context
// is cancelled mid-wait. waitLoop reserves first and only consumes once every
// child reports capacity, and it checks the context before consuming anything.
//
// The wait is not atomic: a child can still deny in the Allow pass if another
// caller races it in between, exactly as Multi.Allow documents. What is ruled out
// is spending a token on a call that then reports an error.
func (m *multiLimiter) Wait(ctx context.Context) error {
	return waitLoop(ctx, m)
}

// Keyed hands out one Limiter per key and bounds how many it remembers.
//
// A per-tenant rate limit is the motivating case: each tenant needs its own
// bucket, but an unbounded map keyed by attacker-controlled input is a memory
// leak. Keyed evicts the least-recently-used limiter once it holds maxKeys.
//
// Keyed deliberately does NOT implement Limiter: a limiter needs a key to be
// meaningful, so callers write k.Get(key).Allow() and the compiler prevents the
// mistake of treating the collection as a single limiter.
type Keyed struct {
	mu       sync.Mutex
	new      func() Limiter
	maxKeys  int
	limiters map[string]*list.Element
	lru      *list.List // front = most recently used
}

// keyedEntry is the value stored in the LRU list.
type keyedEntry struct {
	key string
	lim Limiter
}

// NewKeyed creates a keyed limiter. newLimiter is called to create a limiter for
// a key that is not yet tracked, so the caller controls the policy. maxKeys is
// clamped to at least 1.
//
// A factory per key is what a keyed limiter needs, so the clock is supplied by
// the caller inside newLimiter rather than through an Option: functional options
// would be swallowed here, and a parameter that silently does nothing is worse
// than no parameter at all. Omitting it makes the mistake a compile error,
// matching the reasoning behind Keyed not implementing Limiter.
func NewKeyed(newLimiter func() Limiter, maxKeys int) *Keyed {
	if newLimiter == nil {
		newLimiter = func() Limiter { return NewTokenBucket(1, 1) }
	}
	if maxKeys < 1 {
		maxKeys = 1
	}
	return &Keyed{
		new:      newLimiter,
		maxKeys:  maxKeys,
		limiters: make(map[string]*list.Element, maxKeys),
		lru:      list.New(),
	}
}

// Get returns the limiter for key, creating it if needed and marking it as most
// recently used. When the key count is at capacity, the least-recently-used
// limiter is evicted first.
func (k *Keyed) Get(key string) Limiter {
	k.mu.Lock()
	defer k.mu.Unlock()

	if el, ok := k.limiters[key]; ok {
		k.lru.MoveToFront(el)
		return el.Value.(*keyedEntry).lim
	}

	lim := k.new()
	el := k.lru.PushFront(&keyedEntry{key: key, lim: lim})
	k.limiters[key] = el

	for k.lru.Len() > k.maxKeys {
		oldest := k.lru.Back()
		if oldest == nil {
			break
		}
		k.lru.Remove(oldest)
		delete(k.limiters, oldest.Value.(*keyedEntry).key)
	}
	return lim
}

// Len reports how many keys are currently tracked. It never exceeds maxKeys.
func (k *Keyed) Len() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.lru.Len()
}

// The methods below deliberately do not satisfy the Limiter interface, which
// requires Allow, Reserve, and Wait on the same type. Keyed cannot implement
// that contract meaningfully — a key is always required — so the signatures take
// an extra parameter. This makes the mistake of passing a Keyed where a Limiter
// is expected a compile error rather than a silently unlimited rate.

// Allow takes a key and reports whether that key may proceed now. It is named
// differently from Limiter.Allow so that Keyed is not a Limiter by accident.
func (k *Keyed) Allow(key string) bool { return k.Get(key).Allow() }

// Reserve takes a key and reports how long that key must wait before it may
// proceed. Zero means it may proceed immediately.
func (k *Keyed) Reserve(key string) time.Duration { return k.Get(key).Reserve() }

// Wait takes a key and blocks until that key may proceed or ctx is done.
func (k *Keyed) Wait(ctx context.Context, key string) error { return k.Get(key).Wait(ctx) }
