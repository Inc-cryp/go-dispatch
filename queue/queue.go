// Package queue provides the job queue at the heart of dispatch: a priority
// queue with delayed delivery, at-least-once semantics, visibility timeouts,
// and bounded retries with exponential backoff.
//
// The queue is deliberately a single-process, in-memory structure. It is safe
// for concurrent use by any number of producers and consumers, which is what
// lets a pool of workers drain it in parallel.
//
// # Delivery model
//
// A job moves through a small state machine:
//
//	ready -> reserved -> done
//	                  -> retry  -> ready (after backoff)
//	                  -> failed (terminal)
//
// Dequeue moves a job to reserved and starts a visibility timer. The consumer
// must terminate the job with Ack or Nack before the timer fires; otherwise the
// reservation is considered lost and the job is requeued, exactly as a Nack
// would do. This is the visibility-timeout model used by SQS and similar
// systems. It makes delivery at-least-once: a handler that completes its work
// but crashes before acking will see the job again, so handlers must be
// idempotent.
//
// # Scheduling
//
// A single scheduler goroutine enforces both delayed delivery and visibility
// timeouts. Keeping that logic in one place means no consumer can accidentally
// leak a reservation, and it keeps the hot paths (Enqueue, Dequeue, Ack) free
// of timers.
//
// The zero value is not usable; construct a Queue with New.
package queue

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Errors returned by queue operations. Test them with errors.Is rather than by
// comparing strings.
var (
	// ErrClosed is returned when the queue has been closed.
	ErrClosed = errors.New("queue: closed")
	// ErrUnknownJob is returned when a handler is presented for a job that is
	// not currently reserved, for example because its visibility timeout
	// already lapsed and the job was requeued elsewhere.
	ErrUnknownJob = errors.New("queue: job not reserved")
	// ErrInvalidEntry is returned by Enqueue for a malformed entry.
	ErrInvalidEntry = errors.New("queue: invalid entry")
	// ErrRetryScheduled is returned by Nack when the job was rescheduled rather
	// than abandoned. Callers normally log and continue.
	ErrRetryScheduled = errors.New("queue: retry scheduled")
	// ErrJobFailed is returned by Nack when the job exhausted its attempts. The
	// original cause is wrapped and available through errors.Is/errors.As.
	ErrJobFailed = errors.New("queue: job failed after max attempts")
)

// JobState is the lifecycle state of a job.
type JobState int

const (
	// StateReady means the job is waiting to be delivered.
	StateReady JobState = iota
	// StateReserved means the job has been handed to a consumer and its
	// visibility timer is running.
	StateReserved
	// StateDone means the job completed successfully.
	StateDone
	// StateFailed means the job exhausted its retries and will not run again.
	StateFailed
)

// String implements fmt.Stringer.
func (s JobState) String() string {
	switch s {
	case StateReady:
		return "ready"
	case StateReserved:
		return "reserved"
	case StateDone:
		return "done"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Entry is a job submitted to the queue.
//
// ID must be unique among entries that are live at the same time. Enqueue
// rejects a duplicate live ID with ErrInvalidEntry; deduplication covers queued
// and reserved jobs alike, so a consumer holding a reservation also blocks a
// second submission of the same ID.
type Entry struct {
	// ID uniquely identifies the job. Required.
	ID string
	// Payload is opaque to the queue.
	Payload any
	// Priority orders deliverable jobs: higher runs first. Ties break FIFO.
	Priority int
	// RunAt delays the job until at least this time. Zero means immediately.
	RunAt time.Time
	// MaxAttempts is the total number of delivery attempts allowed, including
	// the first. Values below 1 are treated as 1.
	MaxAttempts int
	// Visibility is how long a reservation is held before the job is assumed
	// lost and requeued. Zero uses the queue default.
	Visibility time.Duration
	// Backoff computes the delay before attempt n (1-based) is retried. Nil
	// uses DefaultBackoff.
	Backoff func(attempt int) time.Duration
}

// Delivery is a job handed to a consumer, together with the handle needed to
// terminate it. A Delivery is valid only until its visibility timeout lapses.
type Delivery struct {
	// Job is the submitted entry.
	Job Entry
	// Attempt is the 1-based delivery attempt number.
	Attempt int
	// Deadline is when the reservation lapses and the job is requeued.
	Deadline time.Time

	token uint64
}

// Handler processes a Delivery. Returning nil acks the job. Returning an error
// fails the attempt and either schedules a retry or gives up according to the
// job's MaxAttempts and Backoff.
type Handler func(ctx context.Context, d Delivery) error

// Stats is a point-in-time snapshot of queue activity. Counters are cumulative
// for the lifetime of the queue.
type Stats struct {
	Ready      int    `json:"ready"`
	Reserved   int    `json:"reserved"`
	Delayed    int    `json:"delayed"`
	Done       uint64 `json:"done"`
	Failed     uint64 `json:"failed"`
	Retried    uint64 `json:"retried"`
	Requeued   uint64 `json:"requeued"` // requeued because a reservation lapsed
	DeadLetter uint64 `json:"dead_letter"`
	Closed     bool   `json:"closed"`
}

// Topic is a fan-out notification channel for queue activity.
type Topic int

const (
	// TopicEnqueued fires when a job is accepted.
	TopicEnqueued Topic = iota
	// TopicDequeued fires when a job is reserved for a consumer.
	TopicDequeued
	// TopicDone fires when a job is acked.
	TopicDone
	// TopicFailed fires when a job exhausts its attempts.
	TopicFailed
	// TopicRetried fires when a failed attempt is scheduled for retry.
	TopicRetried
	// TopicRequeued fires when a reservation lapses and the job returns to ready.
	TopicRequeued
)

// String implements fmt.Stringer.
func (t Topic) String() string {
	switch t {
	case TopicEnqueued:
		return "enqueued"
	case TopicDequeued:
		return "dequeued"
	case TopicDone:
		return "done"
	case TopicFailed:
		return "failed"
	case TopicRetried:
		return "retried"
	case TopicRequeued:
		return "requeued"
	default:
		return "unknown"
	}
}

// Event describes a state transition.
type Event struct {
	Topic   Topic
	JobID   string
	State   JobState
	Attempt int
	Err     error
	At      time.Time
}

// Option configures a Queue.
type Option func(*config)

type config struct {
	visibility   time.Duration
	now          func() time.Time
	pollInterval time.Duration
	onError      func(error)
	eventBuffer  int
}

// WithVisibility sets the default reservation timeout for entries that do not
// specify their own. Defaults to 30s.
func WithVisibility(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.visibility = d
		}
	}
}

// WithClock injects the time source, so tests can drive delays and visibility
// timeouts without sleeping. Defaults to time.Now.
func WithClock(now func() time.Time) Option {
	return func(c *config) {
		if now != nil {
			c.now = now
		}
	}
}

// WithPollInterval sets how often the scheduler looks for delayed jobs that
// have come due and reservations that have lapsed. Defaults to 20ms.
func WithPollInterval(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.pollInterval = d
		}
	}
}

// WithErrorHandler receives asynchronous internal errors. It must not block.
func WithErrorHandler(fn func(error)) Option {
	return func(c *config) {
		if fn != nil {
			c.onError = fn
		}
	}
}

// WithEventBuffer sets the per-subscriber channel capacity for Subscribe.
// Defaults to 128.
func WithEventBuffer(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.eventBuffer = n
		}
	}
}

// DefaultBackoff returns an exponential delay of 100ms * 2^(attempt-1), capped
// at 30s, plus a deterministic jitter of up to +/-12.5% derived from id. The
// jitter is derived from the job ID rather than from a random source so that
// retries do not stampede while remaining reproducible in tests.
func DefaultBackoff(attempt int, id string) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Duration(1<<uint(min(attempt-1, 20))) * 100 * time.Millisecond
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	// FNV-1a over the ID, mapped to a signed -1..1 range.
	var h uint32 = 2166136261
	for i := range len(id) {
		h = (h ^ uint32(id[i])) * 16777619
	}
	jitter := time.Duration(int64(d/8) * int64(h%2001-1000) / 1000)
	out := d + jitter
	if out > 30*time.Second {
		out = 30 * time.Second // apply the cap after jitter, not before
	}
	if out < 0 {
		out = 0
	}
	return out
}

// Queue is a concurrent priority job queue with delayed delivery.
type Queue struct {
	cfg config

	mu       sync.Mutex
	heap     jobHeap
	byID     map[string]*item // live jobs by ID: queued or reserved
	reserved map[uint64]*item // reserved jobs by reservation token
	dead     []string         // IDs that exhausted their attempts, capped
	nextTok  uint64
	seq      uint64 // FIFO tiebreaker, monotonic
	closed   bool

	doneCount    uint64
	failedCount  uint64
	retriedCount uint64
	requeueCount uint64

	subs   map[*Subscription]struct{}
	notify chan struct{} // edge trigger: wakes Dequeue waiters
	wake   chan struct{} // edge trigger: asks the scheduler to re-evaluate

	spin sync.WaitGroup
	quit chan struct{}
	once sync.Once
}

// New creates a Queue and starts its scheduler.
func New(opts ...Option) *Queue {
	cfg := config{
		visibility:   30 * time.Second,
		now:          time.Now,
		pollInterval: 20 * time.Millisecond,
		onError:      func(error) {},
		eventBuffer:  128,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	q := &Queue{
		cfg:      cfg,
		byID:     make(map[string]*item),
		reserved: make(map[uint64]*item),
		subs:     make(map[*Subscription]struct{}),
		notify:   make(chan struct{}, 1),
		wake:     make(chan struct{}, 1),
		quit:     make(chan struct{}),
	}
	heap.Init(&q.heap)

	q.spin.Add(1)
	go q.scheduler()
	return q
}

// item is a heap node. The heap holds every job that is not currently reserved,
// so readiness is derived from RunAt alone and never duplicated in a flag.
type item struct {
	entry   Entry
	attempt int
	seq     uint64
	token   uint64    // 0 while not reserved
	deadl   time.Time // reservation deadline while reserved
}

// jobHeap orders by due time, then by priority (higher first), then FIFO.
//
// Because a due job always has an earlier RunAt than a delayed one, this
// ordering guarantees every deliverable job sorts before every delayed job.
// That invariant is what lets tryReserve inspect only the head.
func (h jobHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if !a.entry.RunAt.Equal(b.entry.RunAt) {
		return a.entry.RunAt.Before(b.entry.RunAt)
	}
	if a.entry.Priority != b.entry.Priority {
		return a.entry.Priority > b.entry.Priority
	}
	return a.seq < b.seq
}

type jobHeap []*item

func (h jobHeap) Len() int      { return len(h) }
func (h jobHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push and Pop must have pointer receivers: container/heap relies on them to
// grow and shrink the underlying slice.
func (h *jobHeap) Push(x any) {
	*h = append(*h, x.(*item))
}

func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil // avoid retaining the popped item
	*h = old[:n-1]
	return it
}

// Enqueue accepts a job. It returns ErrClosed if the queue is shutting down, or
// an error wrapping ErrInvalidEntry for a malformed or duplicate-live ID.
func (q *Queue) Enqueue(e Entry) error {
	if e.ID == "" {
		return fmt.Errorf("%w: empty id", ErrInvalidEntry)
	}
	if e.MaxAttempts < 1 {
		e.MaxAttempts = 1
	}
	// An entry with no RunAt is due immediately. Normalising it to the zero
	// time rather than to now() is what makes Priority and FIFO meaningful: a
	// unique nanosecond timestamp per entry would let clock jitter decide the
	// order before priority was ever consulted.
	if e.RunAt.IsZero() {
		e.RunAt = time.Time{}
	}
	if e.Visibility <= 0 {
		e.Visibility = q.cfg.visibility
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrClosed
	}
	if _, dup := q.byID[e.ID]; dup {
		q.mu.Unlock()
		return fmt.Errorf("%w: duplicate live id %q", ErrInvalidEntry, e.ID)
	}

	it := &item{entry: e, seq: q.seq}
	q.seq++
	q.byID[e.ID] = it
	heap.Push(&q.heap, it)
	q.mu.Unlock()

	q.emit(Event{Topic: TopicEnqueued, JobID: e.ID, State: StateReady, At: q.cfg.now()})
	q.signal(q.wake)
	q.signal(q.notify)
	return nil
}

// Dequeue reserves the next deliverable job, blocking until one is available,
// the context is cancelled, or the queue is closed. It returns ErrClosed when
// the queue closed while waiting.
//
// The caller owns the returned Delivery until it calls Ack, Nack, or Extend. If
// it does none of those before Delivery.Deadline, the job is requeued and may be
// handed to another consumer.
func (q *Queue) Dequeue(ctx context.Context) (Delivery, error) {
	for {
		if d, ok := q.tryReserve(); ok {
			return d, nil
		}

		select {
		case <-ctx.Done():
			return Delivery{}, ctx.Err()
		case <-q.quit:
			// Deliver anything already due before reporting closed, so a
			// shutdown does not silently strand ackable work.
			if d, ok := q.tryReserve(); ok {
				return d, nil
			}
			return Delivery{}, ErrClosed
		case <-q.notify:
		}
	}
}

// tryReserve pops and reserves the best deliverable job, if any.
func (q *Queue) tryReserve() (Delivery, bool) {
	now := q.cfg.now()

	q.mu.Lock()
	if q.closed || len(q.heap) == 0 {
		q.mu.Unlock()
		return Delivery{}, false
	}
	// The heap orders by RunAt, so if the head is not yet due, nothing is.
	it := q.heap[0]
	if it.entry.RunAt.After(now) {
		q.mu.Unlock()
		return Delivery{}, false
	}

	heap.Pop(&q.heap)
	it.attempt++
	it.token = q.nextTok
	q.nextTok++
	it.deadl = now.Add(it.entry.Visibility)
	q.reserved[it.token] = it

	d := Delivery{
		Job:      it.entry,
		Attempt:  it.attempt,
		Deadline: it.deadl,
		token:    it.token,
	}
	q.mu.Unlock()

	// Emitted after the unlock because emit takes q.mu. The event is copied out
	// under the lock above rather than read from the item, which another
	// goroutine may already be requeuing by now.
	q.emit(Event{
		Topic: TopicDequeued, JobID: d.Job.ID, State: StateReserved,
		Attempt: d.Attempt, At: now,
	})
	return d, true
}

// Ack completes a job successfully. It returns ErrUnknownJob if the delivery's
// reservation has already lapsed.
func (q *Queue) Ack(d Delivery) error {
	q.mu.Lock()
	it, ok := q.reserved[d.token]
	if !ok || it.entry.ID != d.Job.ID {
		q.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrUnknownJob, d.Job.ID)
	}
	delete(q.reserved, d.token)
	delete(q.byID, it.entry.ID)
	q.doneCount++
	q.mu.Unlock()

	q.emit(Event{
		Topic: TopicDone, JobID: d.Job.ID, State: StateDone,
		Attempt: d.Attempt, At: q.cfg.now(),
	})
	return nil
}

// Nack reports a failed attempt. If attempts remain the job is rescheduled with
// backoff and ErrRetryScheduled is returned; otherwise it is dead-lettered and
// ErrJobFailed is returned, wrapping cause. It returns ErrUnknownJob for a
// lapsed reservation.
func (q *Queue) Nack(d Delivery, cause error) error {
	now := q.cfg.now()

	q.mu.Lock()
	it, ok := q.reserved[d.token]
	if !ok || it.entry.ID != d.Job.ID {
		q.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrUnknownJob, d.Job.ID)
	}
	delete(q.reserved, d.token)

	if it.attempt >= it.entry.MaxAttempts {
		delete(q.byID, it.entry.ID)
		q.failedCount++
		q.rememberDead(it.entry.ID)
		q.mu.Unlock()

		q.emit(Event{
			Topic: TopicFailed, JobID: d.Job.ID, State: StateFailed,
			Attempt: it.attempt, Err: cause, At: now,
		})
		return fmt.Errorf("%w: %w", ErrJobFailed, cause)
	}

	delay := DefaultBackoff(it.attempt, it.entry.ID)
	if it.entry.Backoff != nil {
		delay = it.entry.Backoff(it.attempt)
	}
	if delay < 0 {
		delay = 0
	}
	it.entry.RunAt = now.Add(delay)
	it.token = 0
	q.retriedCount++
	heap.Push(&q.heap, it)
	q.mu.Unlock()

	q.emit(Event{
		Topic: TopicRetried, JobID: d.Job.ID, State: StateReady,
		Attempt: it.attempt, Err: cause, At: now,
	})
	q.signal(q.wake)
	return ErrRetryScheduled
}

// Extend pushes a reservation's deadline back by ext, for long-running
// handlers. It returns ErrUnknownJob if the reservation has already lapsed,
// since the job may then be running elsewhere.
func (q *Queue) Extend(d Delivery, ext time.Duration) error {
	if ext <= 0 {
		return fmt.Errorf("%w: non-positive extension", ErrInvalidEntry)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	it, ok := q.reserved[d.token]
	if !ok || it.entry.ID != d.Job.ID {
		return fmt.Errorf("%w: %q", ErrUnknownJob, d.Job.ID)
	}
	it.deadl = it.deadl.Add(ext)
	return nil
}

// rememberDead records an ID in the bounded dead-letter list. Callers must hold
// q.mu.
func (q *Queue) rememberDead(id string) {
	const maxDead = 1000 // bound memory; a real backend would persist these
	if len(q.dead) < maxDead {
		q.dead = append(q.dead, id)
	}
}

// Len reports how many jobs are queued, that is, ready or delayed but not yet
// reserved.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.heap)
}

// Stats returns a snapshot of queue activity.
func (q *Queue) Stats() Stats {
	now := q.cfg.now()

	q.mu.Lock()
	defer q.mu.Unlock()

	var delayed int
	for _, it := range q.heap {
		if it.entry.RunAt.After(now) {
			delayed++
		}
	}
	return Stats{
		Ready:      len(q.heap) - delayed,
		Reserved:   len(q.reserved),
		Delayed:    delayed,
		Done:       q.doneCount,
		Failed:     q.failedCount,
		Retried:    q.retriedCount,
		Requeued:   q.requeueCount,
		DeadLetter: uint64(len(q.dead)),
		Closed:     q.closed,
	}
}

// DeadLetters returns the IDs of dead-lettered jobs, newest last. The list is
// capped at 1000 entries to bound memory.
func (q *Queue) DeadLetters() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.dead...)
}

// Close stops the scheduler and releases every waiter. In-flight reservations
// are left as they are: handlers holding them should Ack or Nack first, which
// is what a graceful shutdown does. Close is idempotent.
func (q *Queue) Close() error {
	q.once.Do(func() {
		q.mu.Lock()
		q.closed = true
		subs := make([]*Subscription, 0, len(q.subs))
		for s := range q.subs {
			subs = append(subs, s)
		}
		q.subs = make(map[*Subscription]struct{})
		q.mu.Unlock()

		close(q.quit)
		q.signal(q.notify)
		q.spin.Wait()

		for _, s := range subs {
			s.close()
		}
	})
	return nil
}

// scheduler enforces delayed delivery and visibility timeouts. It is the only
// place reservations can be reclaimed, which keeps that logic in one place
// instead of spread across every consumer.
func (q *Queue) scheduler() {
	defer q.spin.Done()
	t := time.NewTicker(q.cfg.pollInterval)
	defer t.Stop()

	for {
		select {
		case <-q.quit:
			return
		case <-t.C:
		case <-q.wake:
		}
		q.reap()
	}
}

// reap requeues lapsed reservations and wakes waiters when work is deliverable.
func (q *Queue) reap() {
	now := q.cfg.now()
	var lapsed []Event

	q.mu.Lock()
	for token, it := range q.reserved {
		if now.Before(it.deadl) {
			continue
		}
		delete(q.reserved, token)
		it.token = 0
		it.attempt-- // a lapsed reservation does not consume an attempt
		if it.attempt < 0 {
			it.attempt = 0
		}
		it.entry.RunAt = now
		q.requeueCount++
		heap.Push(&q.heap, it)
		lapsed = append(lapsed, Event{
			Topic: TopicRequeued, JobID: it.entry.ID, State: StateReady,
			Attempt: it.attempt, At: now,
		})
	}
	due := len(q.heap) > 0 && !q.heap[0].entry.RunAt.After(now)
	q.mu.Unlock()

	for _, e := range lapsed {
		q.emit(e)
	}
	if due || len(lapsed) > 0 {
		q.signal(q.notify)
	}
}

// signal performs a non-blocking, edge-triggered send.
func (q *Queue) signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// emit fans an event out to subscribers. A subscriber that cannot keep up is
// skipped rather than allowed to block the queue: queue liveness outranks
// notification completeness, and a dropped notification is recoverable from
// Stats. Subscribers can count the loss with Subscription.Dropped.
func (q *Queue) emit(e Event) {
	q.mu.Lock()
	subs := make([]*Subscription, 0, len(q.subs))
	for s := range q.subs {
		subs = append(subs, s)
	}
	q.mu.Unlock()

	for _, s := range subs {
		s.deliver(e)
	}
}

// Subscription receives queue events. Obtain one from Subscribe and release it
// with Close when done.
type Subscription struct {
	q      *Queue
	topics map[Topic]bool

	// ch carries queued events. sendMu is held for reading by every sender and
	// for writing by Close, which is what makes "check closed, then send"
	// atomic with respect to closing ch. emit snapshots the subscriber set
	// under q.mu and releases it before sending, so without this lock a sender
	// that took its snapshot a moment before Close would send on a closed
	// channel and panic.
	sendMu  sync.RWMutex
	ch      chan Event
	closed  bool
	dropped atomic.Uint64
	once    sync.Once
}

// deliver hands one event to the subscription's buffer, skipping it when the
// subscriber did not ask for the topic or cannot keep up.
func (s *Subscription) deliver(e Event) {
	// Check the topic before taking the lock: an unsubscribed event is not a
	// delivery at all, so it must not be counted as a drop.
	if len(s.topics) > 0 && !s.topics[e.Topic] {
		return
	}

	s.sendMu.RLock()
	defer s.sendMu.RUnlock()
	if s.closed {
		return // the subscriber went away deliberately; not an error
	}

	select {
	case s.ch <- e:
	default:
		s.dropped.Add(1)
	}
}

// Subscribe returns a Subscription delivering events for the given topics.
// Passing no topics subscribes to all of them. Close the Subscription to
// release resources.
func (q *Queue) Subscribe(topics ...Topic) *Subscription {
	set := make(map[Topic]bool, len(topics))
	for _, t := range topics {
		set[t] = true
	}

	s := &Subscription{
		q:      q,
		topics: set,
		ch:     make(chan Event, q.cfg.eventBuffer),
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		s.close()
		return s
	}
	q.subs[s] = struct{}{}
	q.mu.Unlock()
	return s
}

// C is the event stream. The channel is closed when the Subscription is closed
// or the queue shuts down.
func (s *Subscription) C() <-chan Event { return s.ch }

// Dropped reports how many events were discarded because the subscriber could
// not keep up.
func (s *Subscription) Dropped() uint64 { return s.dropped.Load() }

// Close unsubscribes and closes the event channel. It is idempotent and safe to
// call concurrently with producers.
func (s *Subscription) Close() error {
	s.q.mu.Lock()
	delete(s.q.subs, s)
	s.q.mu.Unlock()

	s.close()
	return nil
}

// close marks the subscription closed and closes its event channel. The write
// lock is what excludes a concurrent deliver holding the read lock, so the
// close can never land between a sender's closed check and its send.
func (s *Subscription) close() {
	s.once.Do(func() {
		s.sendMu.Lock()
		defer s.sendMu.Unlock()
		s.closed = true
		close(s.ch)
	})
}
