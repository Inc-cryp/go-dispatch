package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock. Using it instead of time.Sleep keeps
// the scheduling tests deterministic and fast.
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

// waitFor polls cond until it holds or the deadline passes. Tests use a
// generous margin so they do not flake on a loaded machine.
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

func TestEnqueueRejectsInvalidEntry(t *testing.T) {
	q := New()
	defer q.Close()

	tests := []struct {
		name string
		id   string
		want error
	}{
		{"empty id", "", ErrInvalidEntry},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := q.Enqueue(Entry{ID: tc.id})
			if !errors.Is(err, tc.want) {
				t.Fatalf("Enqueue(%q) = %v, want %v", tc.id, err, tc.want)
			}
		})
	}
}

func TestEnqueueRejectsDuplicateLiveID(t *testing.T) {
	q := New()
	defer q.Close()

	if err := q.Enqueue(Entry{ID: "a"}); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	if err := q.Enqueue(Entry{ID: "a"}); !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("duplicate Enqueue = %v, want ErrInvalidEntry", err)
	}

	// A reserved job also blocks a duplicate submission.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := q.Dequeue(ctx); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := q.Enqueue(Entry{ID: "a"}); !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("Enqueue while reserved = %v, want ErrInvalidEntry", err)
	}
}

func TestPriorityOrdering(t *testing.T) {
	q := New()
	defer q.Close()

	// Ties on RunAt break on priority, higher first; equal priority is FIFO.
	for _, e := range []Entry{
		{ID: "low", Priority: 1},
		{ID: "high", Priority: 10},
		{ID: "mid-first", Priority: 5},
		{ID: "mid-second", Priority: 5},
	} {
		if err := q.Enqueue(e); err != nil {
			t.Fatalf("Enqueue(%s): %v", e.ID, err)
		}
	}

	ctx := context.Background()
	var got []string
	for range 4 {
		d, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("Dequeue: %v", err)
		}
		got = append(got, d.Job.ID)
		if err := q.Ack(d); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}

	want := []string{"high", "mid-first", "mid-second", "low"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dequeue order = %v, want %v", got, want)
		}
	}
}

func TestDelayedJobIsNotDeliverableEarly(t *testing.T) {
	clk := newFakeClock()
	q := New(WithClock(clk.Now), WithPollInterval(time.Millisecond))
	defer q.Close()

	if err := q.Enqueue(Entry{ID: "later", RunAt: clk.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	stats := q.Stats()
	if stats.Delayed != 1 || stats.Ready != 0 {
		t.Fatalf("stats = %+v, want 1 delayed / 0 ready", stats)
	}

	// A short-deadline Dequeue must time out while the job is not yet due.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := q.Dequeue(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dequeue before RunAt = %v, want DeadlineExceeded", err)
	}

	clk.Advance(2 * time.Hour)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	d, err := q.Dequeue(ctx2)
	if err != nil {
		t.Fatalf("Dequeue after RunAt: %v", err)
	}
	if d.Job.ID != "later" {
		t.Fatalf("got job %q, want %q", d.Job.ID, "later")
	}
}

func TestVisibilityTimeoutRequeuesJob(t *testing.T) {
	clk := newFakeClock()
	q := New(WithClock(clk.Now), WithPollInterval(time.Millisecond))
	defer q.Close()

	if err := q.Enqueue(Entry{ID: "work", Visibility: time.Minute}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("first Dequeue: %v", err)
	}

	// Let the reservation lapse without acking, as a crashed consumer would.
	clk.Advance(2 * time.Minute)

	second, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("second Dequeue: %v", err)
	}
	if second.Job.ID != "work" {
		t.Fatalf("got job %q, want the requeued %q", second.Job.ID, "work")
	}
	if second.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1: a lapsed reservation must not consume an attempt", second.Attempt)
	}

	// The stale delivery must no longer be ackable.
	if err := q.Ack(first); !errors.Is(err, ErrUnknownJob) {
		t.Fatalf("Ack(stale) = %v, want ErrUnknownJob", err)
	}
	if err := q.Ack(second); err != nil {
		t.Fatalf("Ack(current): %v", err)
	}
	if got := q.Stats().Requeued; got != 1 {
		t.Fatalf("Requeued = %d, want 1", got)
	}
}

func TestExtendPostponesVisibilityTimeout(t *testing.T) {
	clk := newFakeClock()
	q := New(WithClock(clk.Now), WithPollInterval(time.Millisecond))
	defer q.Close()

	if err := q.Enqueue(Entry{ID: "long", Visibility: time.Minute}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	d, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	if err := q.Extend(d, time.Hour); err != nil {
		t.Fatalf("Extend: %v", err)
	}

	clk.Advance(2 * time.Minute) // past the original deadline, inside the extension
	if err := q.Ack(d); err != nil {
		t.Fatalf("Ack after Extend = %v, want nil", err)
	}
}

func TestNackRetriesThenDeadLetters(t *testing.T) {
	clk := newFakeClock()
	q := New(WithClock(clk.Now), WithPollInterval(time.Millisecond))
	defer q.Close()

	cause := errors.New("boom")
	backoffCalled := 0
	entry := Entry{
		ID:          "flaky",
		MaxAttempts: 3,
		Backoff: func(attempt int) time.Duration {
			backoffCalled++
			return time.Duration(attempt) * time.Second
		},
	}
	if err := q.Enqueue(entry); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for attempt := 1; attempt <= 3; attempt++ {
		d, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("Dequeue attempt %d: %v", attempt, err)
		}
		if d.Attempt != attempt {
			t.Fatalf("attempt = %d, want %d", d.Attempt, attempt)
		}

		err = q.Nack(d, cause)
		if attempt < 3 {
			if !errors.Is(err, ErrRetryScheduled) {
				t.Fatalf("Nack attempt %d = %v, want ErrRetryScheduled", attempt, err)
			}
			clk.Advance(10 * time.Second) // clear the backoff
			continue
		}
		// Final attempt exhausts MaxAttempts.
		if !errors.Is(err, ErrJobFailed) {
			t.Fatalf("final Nack = %v, want ErrJobFailed", err)
		}
		if !errors.Is(err, cause) {
			t.Fatalf("final Nack error %v does not wrap the cause", err)
		}
	}

	if backoffCalled != 2 {
		t.Fatalf("Backoff called %d times, want 2 (only retries, not the final failure)", backoffCalled)
	}
	if dead := q.DeadLetters(); len(dead) != 1 || dead[0] != "flaky" {
		t.Fatalf("DeadLetters = %v, want [flaky]", dead)
	}
	stats := q.Stats()
	if stats.Failed != 1 || stats.Retried != 2 {
		t.Fatalf("stats = %+v, want 1 failed / 2 retried", stats)
	}
	// A dead-lettered ID is no longer live, so it may be resubmitted.
	if err := q.Enqueue(Entry{ID: "flaky"}); err != nil {
		t.Fatalf("Enqueue after dead-letter = %v, want nil", err)
	}
}

func TestDefaultBackoffIsBoundedAndJitteredDeterministically(t *testing.T) {
	if a, b := DefaultBackoff(5, "job-1"), DefaultBackoff(5, "job-1"); a != b {
		t.Fatalf("DefaultBackoff is not deterministic: %v != %v", a, b)
	}
	if a, b := DefaultBackoff(5, "job-1"), DefaultBackoff(5, "job-2"); a == b {
		t.Fatalf("expected jitter to differ between ids, both were %v", a)
	}
	// 100ms * 2^19 would be enormous; the cap must hold.
	if got := DefaultBackoff(50, "x"); got > 30*time.Second {
		t.Fatalf("DefaultBackoff(50) = %v, want <= 30s", got)
	}
	if got := DefaultBackoff(1, "x"); got < 80*time.Millisecond || got > 120*time.Millisecond {
		t.Fatalf("DefaultBackoff(1) = %v, want roughly 100ms", got)
	}
}

func TestEventsAreDelivered(t *testing.T) {
	q := New()
	defer q.Close()

	sub := q.Subscribe(TopicEnqueued, TopicDone)
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := q.Enqueue(Entry{ID: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	d, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := q.Ack(d); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	var seen []Topic
	waitFor(t, "enqueued and done events", func() bool {
		select {
		case e := <-sub.C():
			seen = append(seen, e.Topic)
			return len(seen) == 2
		default:
			return false
		}
	})

	if seen[0] != TopicEnqueued || seen[1] != TopicDone {
		t.Fatalf("events = %v, want [enqueued done]", seen)
	}
}

func TestSubscriptionDropCountingAndFiltering(t *testing.T) {
	// Tiny buffer plus no draining forces drops rather than blocking the queue.
	q := New(WithEventBuffer(1))
	defer q.Close()

	all := q.Subscribe() // subscribed to every topic, but never drained
	defer all.Close()

	for i := range 50 {
		if err := q.Enqueue(Entry{ID: fmt.Sprintf("j%d", i)}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	if got := all.Dropped(); got == 0 {
		t.Fatal("Dropped() = 0, want > 0 with an undrained 1-slot subscriber")
	}
	// The queue must remain usable despite the stalled subscriber.
	if got := q.Len(); got != 50 {
		t.Fatalf("Len() = %d, want 50", got)
	}
}

func TestCloseIsIdempotentAndReleasesWaiters(t *testing.T) {
	q := New()

	done := make(chan error, 1)
	go func() {
		_, err := q.Dequeue(context.Background())
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Dequeue after Close = %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Dequeue did not return after Close: leaked waiter")
	}

	if err := q.Enqueue(Entry{ID: "x"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue after Close = %v, want ErrClosed", err)
	}
	if !q.Stats().Closed {
		t.Fatal("Stats().Closed = false after Close")
	}
}

func TestContextCancellationUnblocksDequeue(t *testing.T) {
	q := New()
	defer q.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err := q.Dequeue(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Dequeue with cancelled ctx = %v, want context.Canceled", err)
	}
}

// TestConcurrentProducersAndConsumers is the race-detector workhorse: many
// goroutines enqueue and drain simultaneously.
func TestConcurrentProducersAndConsumers(t *testing.T) {
	q := New(WithVisibility(5*time.Second), WithPollInterval(5*time.Millisecond))
	defer q.Close()

	const (
		producers = 8
		consumers = 8
		perProd   = 200
	)
	total := producers * perProd

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var consumed atomic.Int64
	var wg sync.WaitGroup

	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				d, err := q.Dequeue(ctx)
				if err != nil {
					return // ctx done or queue closed
				}
				consumed.Add(1)
				if err := q.Ack(d); err != nil {
					t.Errorf("Ack: %v", err)
					return
				}
			}
		}()
	}

	var prodWG sync.WaitGroup
	for p := range producers {
		prodWG.Add(1)
		go func() {
			defer prodWG.Done()
			for i := range perProd {
				e := Entry{ID: fmt.Sprintf("p%d-j%d", p, i)}
				if err := q.Enqueue(e); err != nil {
					t.Errorf("Enqueue(%s): %v", e.ID, err)
					return
				}
			}
		}()
	}
	prodWG.Wait()

	waitFor(t, "all jobs consumed", func() bool {
		return consumed.Load() == int64(total)
	})

	stats := q.Stats()
	if stats.Done != uint64(total) {
		t.Fatalf("Done = %d, want %d", stats.Done, total)
	}
	if stats.Ready != 0 || stats.Reserved != 0 {
		t.Fatalf("stats = %+v, want an empty queue", stats)
	}

	cancel()
	wg.Wait() // every consumer goroutine must exit
}

func TestStatsTracksDelayedAndReadySeparately(t *testing.T) {
	clk := newFakeClock()
	q := New(WithClock(clk.Now), WithPollInterval(time.Millisecond))
	defer q.Close()

	if err := q.Enqueue(Entry{ID: "now"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := q.Enqueue(Entry{ID: "soon", RunAt: clk.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if got := q.Stats(); got.Ready != 1 || got.Delayed != 1 {
		t.Fatalf("stats = %+v, want 1 ready / 1 delayed", got)
	}
	if got := q.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}

	clk.Advance(2 * time.Minute)
	waitFor(t, "delayed job to become ready", func() bool {
		return q.Stats().Ready == 2
	})
}
