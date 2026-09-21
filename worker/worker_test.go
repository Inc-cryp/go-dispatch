package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Inc-cryp/go-dispatch/queue"
	"github.com/Inc-cryp/go-dispatch/ratelimit"
)

// fakeSink is a scriptable Sink. A real queue cannot be made to fail Ack or
// Nack on demand, so the pool's error paths are exercised against this.
type fakeSink struct {
	mu sync.Mutex

	jobs []queue.Delivery
	next int

	// wake is signalled whenever a job is added, so a Dequeue parked on an
	// empty sink notices new work instead of blocking until ctx ends.
	wake chan struct{}

	acks   []queue.Delivery
	nacks  []nackCall
	extend []queue.Delivery

	ackErr    error
	nackErr   error
	deqErr    error
	extendErr error

	deqDelay time.Duration
}

type nackCall struct {
	d     queue.Delivery
	cause error
}

func newFakeSink(entries ...queue.Entry) *fakeSink {
	f := &fakeSink{wake: make(chan struct{}, 1)}
	for _, e := range entries {
		f.jobs = append(f.jobs, queue.Delivery{
			Job:      e,
			Attempt:  1,
			Deadline: time.Now().Add(time.Hour),
		})
	}
	return f
}

// push appends a job and wakes any parked Dequeue call.
func (f *fakeSink) push(e queue.Entry) {
	f.mu.Lock()
	f.jobs = append(f.jobs, queue.Delivery{
		Job:      e,
		Attempt:  1,
		Deadline: time.Now().Add(time.Hour),
	})
	f.mu.Unlock()

	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *fakeSink) Dequeue(ctx context.Context) (queue.Delivery, error) {
	if d := f.deqDelay; d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return queue.Delivery{}, ctx.Err()
		}
	}

	for {
		f.mu.Lock()
		if f.deqErr != nil {
			err := f.deqErr
			f.mu.Unlock()
			return queue.Delivery{}, err
		}
		if f.next < len(f.jobs) {
			d := f.jobs[f.next]
			f.next++
			f.mu.Unlock()
			return d, nil
		}
		f.mu.Unlock()

		// Nothing to hand out: park until a job arrives or the context ends,
		// mirroring a real queue's blocking Dequeue.
		select {
		case <-f.wake:
		case <-ctx.Done():
			return queue.Delivery{}, ctx.Err()
		}
	}
}

func (f *fakeSink) Ack(d queue.Delivery) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, d)
	return f.ackErr
}

func (f *fakeSink) Nack(d queue.Delivery, cause error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacks = append(f.nacks, nackCall{d: d, cause: cause})
	return f.nackErr
}

func (f *fakeSink) Extend(d queue.Delivery, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.extend = append(f.extend, d)
	return f.extendErr
}

func (f *fakeSink) counts() (acks, nacks int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.acks), len(f.nacks)
}

// waitDrained waits until the sink has handed out everything it has and the
// pool reports no work in flight.
func waitDrained(t *testing.T, f *fakeSink, p *Pool, wantAcks int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		acks, nacks := f.counts()
		if acks+nacks >= wantAcks {
			return
		}
		time.Sleep(time.Millisecond)
	}
	acks, nacks := f.counts()
	t.Fatalf("timed out: acks=%d nacks=%d, want %d terminations", acks, nacks, wantAcks)
}

func TestNewRequiresHandler(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("New with a nil handler should panic, not silently no-op every job")
		}
	}()
	New(newFakeSink())
}

func TestNewPanicsOnNilSink(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("New(nil) should panic")
		}
	}()
	New(nil, WithHandler(func(context.Context, queue.Delivery) error { return nil }))
}

func TestStartRequiresContext(t *testing.T) {
	p := New(newFakeSink(), WithHandler(func(context.Context, queue.Delivery) error { return nil }))
	defer p.Shutdown(context.Background())

	//lint:ignore SA1012 Passing nil is exactly what this test verifies.
	//nolint:staticcheck // SA1012: passing nil is exactly what this test verifies.
	if err := p.Start(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Start(nil) = %v, want ErrNilContext", err)
	}
}

func TestStartTwiceReturnsErrAlreadyStarted(t *testing.T) {
	p := New(newFakeSink(), WithHandler(func(context.Context, queue.Delivery) error { return nil }))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := p.Start(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start = %v, want ErrAlreadyStarted", err)
	}
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestStartAfterShutdownReturnsErrPoolClosed(t *testing.T) {
	p := New(newFakeSink(), WithHandler(func(context.Context, queue.Delivery) error { return nil }))

	ctx := context.Background()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := p.Start(ctx); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Start after Shutdown = %v, want ErrPoolClosed", err)
	}
}

func TestSuccessfulJobsAreAcked(t *testing.T) {
	entries := []queue.Entry{{ID: "a", MaxAttempts: 1}, {ID: "b", MaxAttempts: 1}}
	sink := newFakeSink(entries...)

	var seen sync.Map
	p := New(sink, WithWorkers(2), WithHandler(func(_ context.Context, d queue.Delivery) error {
		seen.Store(d.Job.ID, true)
		return nil
	}))
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitDrained(t, sink, p, 2)
	p.Shutdown(context.Background())

	acks, nacks := sink.counts()
	if acks != 2 || nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want 2 acks and 0 nacks", acks, nacks)
	}
	for _, id := range []string{"a", "b"} {
		if _, ok := seen.Load(id); !ok {
			t.Fatalf("handler never saw job %q", id)
		}
	}

	st := p.Stats()
	if st.Succeeded != 2 || st.Failed != 0 || st.Processed != 2 {
		t.Fatalf("Stats = %+v, want 2 succeeded / 0 failed / 2 processed", st)
	}
}

func TestFailingJobsAreNackedWithTheCause(t *testing.T) {
	sentinel := errors.New("job blew up")
	sink := newFakeSink(queue.Entry{ID: "bad", MaxAttempts: 3})

	p := New(sink, WithHandler(func(context.Context, queue.Delivery) error { return sentinel }))
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitDrained(t, sink, p, 1)
	p.Shutdown(context.Background())

	acks, nacks := sink.counts()
	if acks != 0 || nacks != 1 {
		t.Fatalf("acks=%d nacks=%d, want 0 acks and 1 nack", acks, nacks)
	}

	sink.mu.Lock()
	cause := sink.nacks[0].cause
	sink.mu.Unlock()
	if !errors.Is(cause, sentinel) {
		t.Fatalf("Nack cause = %v, want it to wrap %v", cause, sentinel)
	}

	if st := p.Stats(); st.Failed != 1 {
		t.Fatalf("Stats.Failed = %d, want 1", st.Failed)
	}
}

func TestHandlerPanicIsRecoveredAndNacked(t *testing.T) {
	sink := newFakeSink(queue.Entry{ID: "panicky", MaxAttempts: 5})

	var afterwards atomic.Int64
	p := New(sink, WithHandler(func(_ context.Context, d queue.Delivery) error {
		if d.Job.ID == "panicky" {
			panic("handler exploded")
		}
		afterwards.Add(1)
		return nil
	}))
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitDrained(t, sink, p, 1)

	// The pool must still be alive and usable after a panic: feed it another
	// job and require it to be handled.
	sink.push(queue.Entry{ID: "after", MaxAttempts: 1})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && afterwards.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	if afterwards.Load() != 1 {
		t.Fatal("the worker pool did not survive a handler panic")
	}

	p.Shutdown(context.Background())

	sink.mu.Lock()
	var cause error
	for _, n := range sink.nacks {
		if n.d.Job.ID == "panicky" {
			cause = n.cause
		}
	}
	sink.mu.Unlock()
	if cause == nil {
		t.Fatal("a panicking job must be nacked, not silently dropped")
	}
	if !errors.Is(cause, ErrHandlerPanic) {
		t.Fatalf("Nack cause = %v, want it to wrap ErrHandlerPanic", cause)
	}
}

func TestPanicInOnResultDoesNotKillThePool(t *testing.T) {
	sink := newFakeSink(
		queue.Entry{ID: "one", MaxAttempts: 1},
		queue.Entry{ID: "two", MaxAttempts: 1},
	)

	var results atomic.Int64
	p := New(sink,
		WithWorkers(1),
		WithHandler(func(context.Context, queue.Delivery) error { return nil }),
		WithOnResult(func(Result) {
			// Panic only on the first result, so the pool must recover and
			// carry on to the second job.
			if results.Add(1) == 1 {
				panic("callback exploded")
			}
		}),
	)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitDrained(t, sink, p, 2)
	p.Shutdown(context.Background())

	if got := results.Load(); got != 2 {
		t.Fatalf("OnResult invocations = %d, want 2: a panicking callback must not "+
			"take down the worker", got)
	}
	if st := p.Stats(); st.Succeeded != 2 {
		t.Fatalf("Stats.Succeeded = %d, want 2", st.Succeeded)
	}
}

func TestRetryScheduledIsCountedSeparatelyFromDeadLetter(t *testing.T) {
	sink := newFakeSink(queue.Entry{ID: "retryable", MaxAttempts: 3})
	sink.nackErr = queue.ErrRetryScheduled

	var got []Result
	var mu sync.Mutex

	p := New(sink,
		WithHandler(func(context.Context, queue.Delivery) error { return errors.New("nope") }),
		WithOnResult(func(r Result) {
			mu.Lock()
			got = append(got, r)
			mu.Unlock()
		}),
	)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitDrained(t, sink, p, 1)
	p.Shutdown(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("results = %d, want 1", len(got))
	}
	if !got[0].RetryScheduled || got[0].DeadLettered {
		t.Fatalf("Result = %+v, want RetryScheduled=true DeadLettered=false", got[0])
	}
	if st := p.Stats(); st.Retried != 1 || st.Dead != 0 {
		t.Fatalf("Stats = %+v, want 1 retried and 0 dead", st)
	}
}

func TestDeadLetterIsCounted(t *testing.T) {
	sink := newFakeSink(queue.Entry{ID: "doomed", MaxAttempts: 1})
	sink.nackErr = queue.ErrJobFailed

	var mu sync.Mutex
	var got Result

	p := New(sink,
		WithHandler(func(context.Context, queue.Delivery) error { return errors.New("nope") }),
		WithOnResult(func(r Result) {
			mu.Lock()
			got = r
			mu.Unlock()
		}),
	)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitDrained(t, sink, p, 1)
	p.Shutdown(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if !got.DeadLettered || got.RetryScheduled {
		t.Fatalf("Result = %+v, want DeadLettered=true RetryScheduled=false", got)
	}
	if st := p.Stats(); st.Dead != 1 || st.Retried != 0 {
		t.Fatalf("Stats = %+v, want 1 dead and 0 retried", st)
	}
}

func TestDequeueErrorIsReportedAndThePoolKeepsPolling(t *testing.T) {
	sink := newFakeSink()
	fatal := errors.New("backend unavailable")
	sink.deqErr = fatal
	sink.deqDelay = 2 * time.Millisecond

	var buf syncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	p := New(sink,
		WithHandler(func(context.Context, queue.Delivery) error { return nil }),
		WithLogger(logger),
	)
	// The dispatcher must survive a persistently failing Dequeue instead of
	// dying on the first error and silently stopping the pool.
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(100 * time.Millisecond) // let the dispatcher hit the error repeatedly

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown after persistent Dequeue errors: %v", err)
	}
	if st := p.Stats(); st.Processed != 0 {
		t.Fatalf("Processed = %d, want 0", st.Processed)
	}
	if !strings.Contains(buf.String(), "dequeue failed") {
		t.Fatalf("log output does not mention the dequeue failure:\n%s", buf.String())
	}
}

// syncBuffer is a goroutine-safe io.Writer for slog in tests.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestAckErrorIsCountedAsFailure(t *testing.T) {
	sink := newFakeSink(queue.Entry{ID: "ackfail", MaxAttempts: 1})
	sink.ackErr = queue.ErrUnknownJob

	var mu sync.Mutex
	var got Result

	p := New(sink,
		WithHandler(func(context.Context, queue.Delivery) error { return nil }),
		WithOnResult(func(r Result) {
			mu.Lock()
			got = r
			mu.Unlock()
		}),
	)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitDrained(t, sink, p, 1)
	p.Shutdown(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if got.Err == nil {
		t.Fatal("a failed Ack must surface as a Result error, not a success")
	}
	if !errors.Is(got.Err, queue.ErrUnknownJob) {
		t.Fatalf("Result.Err = %v, want it to wrap queue.ErrUnknownJob", got.Err)
	}
	if st := p.Stats(); st.Failed != 1 || st.Succeeded != 0 {
		t.Fatalf("Stats = %+v, want 1 failed and 0 succeeded", st)
	}
}

func TestNackErrorIsCountedAsFailure(t *testing.T) {
	sink := newFakeSink(queue.Entry{ID: "nackfail", MaxAttempts: 1})
	sink.nackErr = errors.New("could not requeue")

	var mu sync.Mutex
	var got Result

	p := New(sink,
		WithHandler(func(context.Context, queue.Delivery) error { return errors.New("handler failed") }),
		WithOnResult(func(r Result) {
			mu.Lock()
			got = r
			mu.Unlock()
		}),
	)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitDrained(t, sink, p, 1)
	p.Shutdown(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if got.Err == nil {
		t.Fatal("a failed Nack must surface as a Result error")
	}
	if got.DeadLettered || got.RetryScheduled {
		t.Fatalf("Result = %+v, want neither DeadLettered nor RetryScheduled when "+
			"the queue never accepted the nack", got)
	}
}

func TestLimiterThrottlesJobStarts(t *testing.T) {
	clk := newStepClock()
	lim := ratelimit.NewTokenBucket(1000, 2, ratelimit.WithClock(clk.Now))

	sink := newFakeSink(
		queue.Entry{ID: "1", MaxAttempts: 1},
		queue.Entry{ID: "2", MaxAttempts: 1},
		queue.Entry{ID: "3", MaxAttempts: 1},
		queue.Entry{ID: "4", MaxAttempts: 1},
	)

	var started atomic.Int64
	p := New(sink,
		WithWorkers(2),
		WithLimiter(lim),
		WithHandler(func(_ context.Context, d queue.Delivery) error {
			started.Add(1)
			return nil
		}),
	)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Let the pool consume the burst. The clock is frozen, so the bucket cannot
	// refill and no more than 2 jobs may ever start.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && started.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // give any over-permissive limiter time to misbehave

	if got := started.Load(); got != 2 {
		t.Fatalf("started = %d, want exactly the burst of 2 from a frozen bucket", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestLimiterWaitIsCancelledOnShutdown(t *testing.T) {
	// A limiter that never allows: everything must unwind on Shutdown rather
	// than deadlock the drain.
	lim := &neverLimiter{}
	sink := newFakeSink(queue.Entry{ID: "stuck", MaxAttempts: 1})

	p := New(sink, WithWorkers(1), WithLimiter(lim),
		WithHandler(func(context.Context, queue.Delivery) error { return nil }))
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(50 * time.Millisecond) // the worker is now parked in Limiter.Wait

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- p.Shutdown(ctx) }()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown = %v, want nil or context.DeadlineExceeded", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Shutdown hung while a worker was parked in Limiter.Wait")
	}
}

// neverLimiter blocks forever in Wait, modelling a limiter with no capacity.
type neverLimiter struct{}

func (neverLimiter) Allow() bool            { return false }
func (neverLimiter) Reserve() time.Duration { return time.Hour }
func (neverLimiter) Wait(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// newStepClock returns a clock that only reports time when aborted; it stays
// frozen so a token bucket cannot refill during a test.
func newStepClock() *stepClock {
	return &stepClock{now: time.Now()}
}

type stepClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func TestShutdownIsIdempotentAndMarksStopped(t *testing.T) {
	sink := newFakeSink()
	p := New(sink, WithHandler(func(context.Context, queue.Delivery) error { return nil }))

	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for range 3 {
		if err := p.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}
	if !p.Stats().Stopped {
		t.Fatal("Shutdown should mark the pool stopped")
	}
}

func TestShutdownDrainsInFlightJobs(t *testing.T) {
	sink := newFakeSink(queue.Entry{ID: "slow", MaxAttempts: 1})

	handlerStarted := make(chan struct{})
	var finished atomic.Bool

	p := New(sink, WithWorkers(1), WithHandler(func(context.Context, queue.Delivery) error {
		close(handlerStarted)
		time.Sleep(150 * time.Millisecond)
		finished.Store(true)
		return nil
	}))
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case <-handlerStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("handler never started")
	}

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !finished.Load() {
		t.Fatal("Shutdown returned before the in-flight job completed: it must drain")
	}
	if acks, _ := sink.counts(); acks != 1 {
		t.Fatalf("acks = %d, want 1: the drained job must still be acked", acks)
	}
}

func TestShutdownHonoursDrainTimeout(t *testing.T) {
	sink := newFakeSink(queue.Entry{ID: "stuck", MaxAttempts: 1})

	release := make(chan struct{})
	p := New(sink,
		WithWorkers(1),
		WithDrainTimeout(100*time.Millisecond),
		WithHandler(func(context.Context, queue.Delivery) error {
			<-release // hold the worker well past the drain timeout
			return nil
		}),
	)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(50 * time.Millisecond) // let the job start

	err := p.Shutdown(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want context.DeadlineExceeded on a stuck job", err)
	}
	close(release)
}

func TestShutdownRespectsContextCancellation(t *testing.T) {
	sink := newFakeSink(queue.Entry{ID: "stuck", MaxAttempts: 1})
	release := make(chan struct{})

	p := New(sink, WithWorkers(1), WithHandler(func(context.Context, queue.Delivery) error {
		<-release
		return nil
	}))
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- p.Shutdown(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Shutdown = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown ignored context cancellation")
	}
	close(release)
}

func TestWorkersAreClampedToOne(t *testing.T) {
	p := New(newFakeSink(), WithWorkers(0),
		WithHandler(func(context.Context, queue.Delivery) error { return nil }))
	defer p.Shutdown(context.Background())

	if got := p.Stats().Workers; got != 1 {
		t.Fatalf("Workers = %d, want it clamped to 1", got)
	}
}

func TestConcurrentShutdownIsRaceFree(t *testing.T) {
	sink := newFakeSink()
	p := New(sink, WithWorkers(2),
		WithHandler(func(context.Context, queue.Delivery) error { return nil }))

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.Shutdown(context.Background())
			_ = p.Stats()
			_ = p.InFlight()
		}()
	}
	wg.Wait()
}

// TestPoolDrainsRealQueue exercises the pool against the real queue.Queue
// instead of the fake, proving the Sink interface matches the concrete type and
// that the end-to-end path (enqueue -> deliver -> ack) works.
func TestPoolDrainsRealQueue(t *testing.T) {
	q := queue.New(queue.WithPollInterval(2 * time.Millisecond))
	defer q.Close()

	const jobs = 50
	for i := range jobs {
		if err := q.Enqueue(queue.Entry{
			ID:          fmt.Sprintf("job-%d", i),
			Payload:     i,
			MaxAttempts: 3,
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	var processed atomic.Int64
	p := New(q, WithWorkers(8), WithHandler(func(_ context.Context, d queue.Delivery) error {
		processed.Add(1)
		return nil
	}))
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && processed.Load() < jobs {
		time.Sleep(2 * time.Millisecond)
	}
	if got := processed.Load(); got != jobs {
		t.Fatalf("processed = %d, want %d", got, jobs)
	}

	p.Shutdown(context.Background())

	if got := q.Len(); got != 0 {
		t.Fatalf("queue length = %d after draining, want 0", got)
	}
	st := p.Stats()
	if st.Succeeded != jobs {
		t.Fatalf("Stats.Succeeded = %d, want %d", st.Succeeded, jobs)
	}
}

// TestPoolRetriesRealQueue drives a job that fails until it exhausts its
// attempts, verifying the dead-letter path end to end.
func TestPoolRetriesRealQueue(t *testing.T) {
	q := queue.New(queue.WithPollInterval(2 * time.Millisecond))
	defer q.Close()

	if err := q.Enqueue(queue.Entry{
		ID:          "flaky",
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return time.Millisecond },
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var attempts atomic.Int64
	p := New(q, WithWorkers(1), WithHandler(func(context.Context, queue.Delivery) error {
		attempts.Add(1)
		return errors.New("always fails")
	}))
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(q.DeadLetters()) == 0 {
		time.Sleep(2 * time.Millisecond)
	}

	p.Shutdown(context.Background())

	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3 (MaxAttempts)", got)
	}
	dls := q.DeadLetters()
	if len(dls) != 1 || dls[0] != "flaky" {
		t.Fatalf("DeadLetters() = %v, want [flaky]", dls)
	}
	if st := p.Stats(); st.Dead != 1 {
		t.Fatalf("Stats.Dead = %d, want 1", st.Dead)
	}
}

// TestShutdownNackIsReportedAsRetryNotFailure is a regression test. A job that
// is cut short by shutdown is Nacked, and the queue answers ErrRetryScheduled —
// which means the job will run again, not that the Nack failed. The pool used
// to log that answer as "nack during shutdown failed", making every ordinary
// graceful shutdown look like it had lost work.
func TestShutdownNackIsReportedAsRetryNotFailure(t *testing.T) {
	// The handler parks until its context is cancelled, which is what a
	// SIGTERM-driven shutdown looks like from inside a handler.
	started := make(chan struct{})

	sink := newFakeSink(queue.Entry{ID: "interrupted", MaxAttempts: 5})
	sink.nackErr = queue.ErrRetryScheduled

	var mu sync.Mutex
	var results []Result

	// The pool deliberately does not cancel the handler context on a graceful
	// Shutdown, so the parent context is what interrupts the handler here —
	// exactly as cmd/dispatchd's signal context does in production.
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	p := New(sink,
		WithWorkers(1),
		WithDrainTimeout(3*time.Second),
		WithHandler(func(ctx context.Context, _ queue.Delivery) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}),
		WithOnResult(func(r Result) {
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}),
	)

	if err := p.Start(parent); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("handler never started")
	}

	cancelParent()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}

	got := results[0]
	if !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("result Err = %v, want an error wrapping context.Canceled", got.Err)
	}
	if !got.RetryScheduled {
		t.Fatal("a Nack answered with ErrRetryScheduled must set RetryScheduled")
	}
	if got.DeadLettered {
		t.Fatal("a rescheduled job must not be reported as dead-lettered")
	}
	if st := p.Stats(); st.Retried != 1 || st.Dead != 0 {
		t.Fatalf("Stats = %+v, want Retried=1 Dead=0", st)
	}
}
