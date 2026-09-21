// Package eventbus implements an in-process, topic-based publish/subscribe bus.
//
// It is one of the three coordination primitives in dispatch, alongside
// ratelimit and worker. The bus decouples producers from consumers: a publisher
// names a topic and the bus routes the event to every subscription whose pattern
// matches, with no compile-time knowledge of who is listening.
//
// # Topic grammar
//
// Topics are dot-separated segments. A subscription pattern may use two
// wildcards:
//
//   - matches exactly one segment
//     >  matches one or more trailing segments
//
// So the pattern "orders.*" matches "orders.created" but not "orders.created.eu",
// while "orders.>" matches both. A ">" is only legal as the final segment.
//
// # Delivery
//
// Publish is asynchronous: it hands the event to each matching subscription and
// returns without waiting for handlers. PublishSync instead runs the handlers on
// the caller's goroutine and waits for all of them, which is useful when the
// publisher needs to know the event was fully processed.
//
// Each subscription has a buffered channel drained by one or more handler
// goroutines. When that buffer is full, the subscription's DropPolicy decides
// what happens: DropNewest discards the event and counts it (visible via
// Dropped), while Block applies backpressure to the publisher. DropNewest is the
// default because a slow observer must not be able to stall the whole system.
//
// The zero value is not usable; construct a Bus with New.
package eventbus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Errors returned by the bus.
var (
	// ErrBusClosed is returned by Publish on a closed bus.
	ErrBusClosed = errors.New("eventbus: bus is closed")
	// ErrInvalidPattern is returned by Subscribe for a malformed topic pattern.
	ErrInvalidPattern = errors.New("eventbus: invalid topic pattern")
	// ErrSubClosed is returned when publishing to a subscription that is closed.
	ErrSubClosed = errors.New("eventbus: subscription is closed")
)

// Event is a message published to the bus.
type Event struct {
	// Topic is the routing key.
	Topic string
	// Payload is application data, opaque to the bus.
	Payload any
	// At is the publish time. Publish fills it with the bus clock when zero.
	At time.Time
}

// Handler processes an event. For an asynchronous Publish, a returned error is
// reported to the ErrorHandler and never propagated to the publisher.
type Handler func(ctx context.Context, e Event) error

// ErrorHandler receives handler errors and recovered handler panics.
type ErrorHandler func(e Event, err error)

// DropPolicy decides what a subscription does when its buffer is full.
type DropPolicy int

const (
	// DropNewest discards the incoming event and increments the dropped counter.
	// This is the default: it keeps a slow observer from stalling publishers.
	DropNewest DropPolicy = iota
	// Block applies backpressure, making the publisher wait for buffer space.
	Block
)

// String implements fmt.Stringer.
func (p DropPolicy) String() string {
	switch p {
	case DropNewest:
		return "drop-newest"
	case Block:
		return "block"
	default:
		return "unknown"
	}
}

type config struct {
	errHandler ErrorHandler
	buffer     int
	workers    int
	now        func() time.Time
}

type subConfig struct {
	buffer     int
	workers    int
	dropPolicy DropPolicy
}

// Option configures a Bus.
type Option func(*config)

// WithErrorHandler sets the callback for handler errors and panics.
func WithErrorHandler(h ErrorHandler) Option {
	return func(c *config) {
		if h != nil {
			c.errHandler = h
		}
	}
}

// WithBuffer sets the default per-subscription buffer size. Defaults to 64.
func WithBuffer(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.buffer = n
		}
	}
}

// WithWorkers sets the default number of handler goroutines per subscription.
// Defaults to 1. Raising it lets a single subscription process events
// concurrently, at the cost of losing ordering within that subscription.
func WithWorkers(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.workers = n
		}
	}
}

// WithClock sets the bus time source. Defaults to time.Now.
func WithClock(now func() time.Time) Option {
	return func(c *config) {
		if now != nil {
			c.now = now
		}
	}
}

// SubOption configures one Subscription.
type SubOption func(*subConfig)

// WithSubBuffer sets this subscription's buffer size.
func WithSubBuffer(n int) SubOption {
	return func(c *subConfig) {
		if n > 0 {
			c.buffer = n
		}
	}
}

// WithSubWorkers sets this subscription's handler goroutine count.
func WithSubWorkers(n int) SubOption {
	return func(c *subConfig) {
		if n > 0 {
			c.workers = n
		}
	}
}

// WithDropPolicy sets this subscription's overflow behaviour.
func WithDropPolicy(p DropPolicy) SubOption {
	return func(c *subConfig) { c.dropPolicy = p }
}

// Bus is a topic-based publish/subscribe hub. It is safe for concurrent use.
type Bus struct {
	cfg config

	mu     sync.RWMutex
	subs   []*Subscription
	nextID uint64
	closed bool
}

// New creates a Bus.
func New(opts ...Option) *Bus {
	cfg := config{
		buffer:  64,
		workers: 1,
		now:     time.Now,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Bus{cfg: cfg}
}

// Subscribe registers h for pattern. It returns ErrInvalidPattern if the pattern
// is malformed. Subscribing to an already-closed bus returns a subscription that
// is immediately closed, so callers need not special-case shutdown.
func (b *Bus) Subscribe(pattern string, h Handler, opts ...SubOption) (*Subscription, error) {
	if !validPattern(pattern) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidPattern, pattern)
	}
	if h == nil {
		return nil, fmt.Errorf("%w: missing handler", ErrInvalidPattern)
	}

	sc := subConfig{
		buffer:     b.cfg.buffer,
		workers:    b.cfg.workers,
		dropPolicy: DropNewest,
	}
	for _, opt := range opts {
		opt(&sc)
	}

	s := &Subscription{
		bus:     b,
		pattern: pattern,
		handler: h,
		policy:  sc.dropPolicy,
		ch:      make(chan Event, sc.buffer),
		closing: make(chan struct{}),
	}

	// Start the workers BEFORE publishing the subscription into the registry.
	// Bus.Close reaches subscriptions through that registry and calls Close,
	// which waits on s.wg; if the registration happened first, a concurrent
	// Bus.Close could Wait() while this function was still calling Add(), which
	// is illegal WaitGroup usage and a real data race.
	for range sc.workers {
		s.wg.Add(1)
		go s.run()
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		s.shutdown() // workers exit as soon as ch is closed
		return s, nil
	}
	b.nextID++
	s.id = b.nextID
	b.subs = append(b.subs, s)
	b.mu.Unlock()

	return s, nil
}

// Publish queues e to every matching subscription and returns without waiting
// for handlers. It returns ErrBusClosed if the bus is closed.
//
// With DropNewest, a full subscription buffer causes that event to be dropped
// and counted; the publisher is not blocked. With Block, Publish waits until
// there is room, the context is done, or the subscription closes.
func (b *Bus) Publish(ctx context.Context, e Event) error {
	if e.At.IsZero() {
		e.At = b.cfg.now()
	}

	subs := b.matching(e.Topic)
	if len(subs) == 0 {
		return b.closedErr()
	}

	var firstErr error
	for _, s := range subs {
		if err := s.deliver(ctx, e); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// PublishSync delivers e to every matching subscription and waits for each
// matching handler to return. Errors from handlers are aggregated.
func (b *Bus) PublishSync(ctx context.Context, e Event) error {
	if e.At.IsZero() {
		e.At = b.cfg.now()
	}

	subs := b.matching(e.Topic)
	if len(subs) == 0 {
		return b.closedErr()
	}

	var errs []error
	for _, s := range subs {
		if s.isClosed() {
			errs = append(errs, fmt.Errorf("%w: %s", ErrSubClosed, s.pattern))
			continue
		}
		if err := s.invoke(ctx, e); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// closedErr reports ErrBusClosed when the bus is closed, else nil.
func (b *Bus) closedErr() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return ErrBusClosed
	}
	return nil
}

// matching returns a snapshot of the subscriptions whose pattern matches topic.
//
// The lock is released before returning, so callers never hold it while
// delivering: a Block-policy send can wait indefinitely, and holding the bus
// lock across that wait would stall every other publisher.
func (b *Bus) matching(topic string) []*Subscription {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*Subscription, 0, len(b.subs))
	for _, s := range b.subs {
		if match(topic, s.pattern) {
			out = append(out, s)
		}
	}
	return out
}

// Close stops every subscription, waits for in-flight handlers, then returns.
// It is idempotent.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	subs := b.subs
	b.subs = nil
	b.mu.Unlock()

	for _, s := range subs {
		_ = s.Close()
	}
	return nil
}

// Subscription is one registration on a Bus. Close it to stop delivery.
type Subscription struct {
	bus     *Bus
	id      uint64
	pattern string
	handler Handler
	policy  DropPolicy

	// ch carries queued events. sendMu is held for reading by every sender and
	// for writing by Close, which is what makes "check closed, then send"
	// atomic with respect to closing ch. Without it, a Publish racing a Close
	// would send on a closed channel.
	sendMu  sync.RWMutex
	ch      chan Event
	closed  bool
	closing chan struct{} // closed first, to unblock Block-policy senders
	wg      sync.WaitGroup
	once    sync.Once

	dropped atomic.Uint64
}

// Topic returns the pattern this subscription registered.
func (s *Subscription) Topic() string { return s.pattern }

// Dropped reports how many events were discarded because the buffer was full.
func (s *Subscription) Dropped() uint64 { return s.dropped.Load() }

// Close stops the subscription after draining queued events, and waits for
// in-flight handlers. It is idempotent and safe to call concurrently.
func (s *Subscription) Close() error {
	s.once.Do(func() {
		// Unblock any Block-policy sender first: it may be parked on a full
		// buffer while holding sendMu for reading, and Close needs the write
		// lock below.
		close(s.closing)

		s.bus.mu.Lock()
		for i, other := range s.bus.subs {
			if other == s {
				s.bus.subs = append(s.bus.subs[:i], s.bus.subs[i+1:]...)
				break
			}
		}
		s.bus.mu.Unlock()

		s.shutdown()
	})
	s.wg.Wait()
	return nil
}

// shutdown marks the subscription closed and closes its event channel exactly
// once. Callers must ensure no sender can be mid-send, either by holding the
// write lock (Close) or by being the sole owner (Subscribe on a closed bus).
func (s *Subscription) shutdown() {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

func (s *Subscription) isClosed() bool {
	s.sendMu.RLock()
	defer s.sendMu.RUnlock()
	return s.closed
}

// deliver hands an event to the subscription's buffer according to its policy.
func (s *Subscription) deliver(ctx context.Context, e Event) error {
	s.sendMu.RLock()
	defer s.sendMu.RUnlock()
	if s.closed {
		return nil // the subscriber went away deliberately; not an error
	}

	if s.policy == Block {
		select {
		case s.ch <- e:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closing:
			return nil
		}
	}

	select {
	case s.ch <- e:
		return nil
	default:
		s.dropped.Add(1)
		return nil
	}
}

// run drains the event channel until it is closed.
func (s *Subscription) run() {
	defer s.wg.Done()
	for e := range s.ch {
		_ = s.invoke(context.Background(), e)
	}
}

// invoke runs the handler with panic recovery and reports any error. Concurrency
// within a subscription is bounded by the number of run goroutines.
func (s *Subscription) invoke(ctx context.Context, e Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("eventbus: handler panicked on topic %q: %v", e.Topic, r)
			s.report(e, err)
		}
	}()

	err = s.handler(ctx, e)
	if err != nil {
		s.report(e, err)
	}
	return err
}

func (s *Subscription) report(e Event, err error) {
	h := s.bus.cfg.errHandler
	if h == nil {
		return
	}
	// The error handler is user code; a panic in it must not take down a worker.
	defer func() { _ = recover() }()
	h(e, err)
}

// validPattern reports whether pattern is well formed.
func validPattern(pattern string) bool {
	if pattern == "" {
		return false
	}
	segs := strings.Split(pattern, ".")
	for i, seg := range segs {
		switch seg {
		case "*":
			// legal in any position
		case ">":
			if i != len(segs)-1 {
				return false // '>' is only legal as the final segment
			}
		case "":
			return false // empty segments such as "a..b" or "a."
		default:
			if strings.ContainsAny(seg, "*>") {
				return false // wildcards may not be embedded in a literal
			}
		}
	}
	return true
}

// match reports whether topic matches pattern.
//
// A ">" matches one or more remaining segments, never zero: pattern "a.>"
// matches "a.b" but not "a". That mirrors the natural reading of "any children".
func match(topic, pattern string) bool {
	tp := strings.Split(topic, ".")
	pp := strings.Split(pattern, ".")

	for i, p := range pp {
		if p == ">" {
			return i < len(tp) // at least one segment must remain
		}
		if i >= len(tp) {
			return false
		}
		if p == "*" {
			continue
		}
		if p != tp[i] {
			return false
		}
	}
	return len(tp) == len(pp)
}
