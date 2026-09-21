// Package worker runs a pool of goroutines that drain a queue.Queue.
//
// It is the concurrency core of dispatch: it owns the lifecycle of the worker
// goroutines, applies backpressure through a ratelimit.Limiter, retries or
// dead-letters failures via the queue, and shuts down gracefully so that
// in-flight jobs finish instead of being abandoned mid-execution.
//
// # Design
//
// One dispatcher goroutine calls Dequeue and hands each Delivery to a free
// worker over a channel. Workers never touch the queue's reservation directly;
// they report the outcome back and the dispatcher (or the worker that ran the
// job) acks or nacks. This keeps the number of goroutines calling into the
// queue small and bounded, which makes the queue's own locking predictable.
//
// The zero value is not usable; construct a Pool with New.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Inc-cryp/go-dispatch/queue"
	"github.com/Inc-cryp/go-dispatch/ratelimit"
)

// Errors returned by pool operations.
var (
	// ErrPoolClosed is returned when the pool is already shut down.
	ErrPoolClosed = errors.New("worker: pool is closed")
	// ErrAlreadyStarted is returned by Start when the pool is already running.
	ErrAlreadyStarted = errors.New("worker: pool already started")
	// ErrNilContext is returned by Start when ctx is nil.
	ErrNilContext = errors.New("worker: nil context")
	// ErrHandlerPanic is wrapped by the error reported when a handler panics.
	// It lets callers tell a buggy handler apart from an ordinary job failure,
	// for example to alert rather than count a retry.
	ErrHandlerPanic = errors.New("worker: handler panicked")
)

// Sink is the subset of *queue.Queue that a Pool needs. Defining it here, where
// it is consumed, lets callers substitute a fake queue in tests and keeps the
// pool from depending on the whole queue package.
type Sink interface {
	Dequeue(ctx context.Context) (queue.Delivery, error)
	Ack(d queue.Delivery) error
	Nack(d queue.Delivery, cause error) error
	Extend(d queue.Delivery, ext time.Duration) error
}

// Config is the pool configuration.
type Config struct {
	// Workers is the number of concurrent job runners. Clamped to >= 1.
	Workers int
	// Handler processes one job. Required.
	Handler queue.Handler
	// Limiter throttles job starts. Nil means unthrottled.
	Limiter ratelimit.Limiter
	// DrainTimeout bounds how long Shutdown waits for in-flight jobs before
	// abandoning them. Zero means wait forever.
	DrainTimeout time.Duration
	// Logger receives lifecycle and error logs. Nil disables logging.
	Logger *slog.Logger
	// OnResult, when set, is called after every job with its outcome. It runs on
	// the worker goroutine and must not block.
	OnResult func(Result)
}

// Result is the outcome of one job attempt.
type Result struct {
	// ID is the job identifier.
	ID string
	// Attempt is the 1-based attempt number.
	Attempt int
	// Duration is how long the handler ran.
	Duration time.Duration
	// Err is nil on success, or the handler error otherwise. RetryScheduled is
	// true when the queue will try the job again.
	Err error
	// RetryScheduled reports whether the queue rescheduled the job for another
	// attempt rather than dead-lettering it.
	RetryScheduled bool
	// DeadLettered reports whether the job exhausted its attempts.
	DeadLettered bool
}

// OK reports whether the attempt succeeded.
func (r Result) OK() bool { return r.Err == nil }

// Pool is a fixed-size worker pool draining a queue.
type Pool struct {
	cfg  Config
	sink Sink

	jobs    chan queue.Delivery
	wg      sync.WaitGroup // one per worker goroutine
	dispWG  sync.WaitGroup // the dispatcher goroutine
	stopped atomic.Bool
	once    sync.Once
	cancel  context.CancelFunc // unblocks Dequeue and limiter waits on Shutdown
	started atomic.Bool

	// Stats
	processed atomic.Uint64
	succeeded atomic.Uint64
	failed    atomic.Uint64
	retried   atomic.Uint64
	dead      atomic.Uint64
}

// Option configures a Pool.
type Option func(*Config)

// WithWorkers sets the number of concurrent runners.
func WithWorkers(n int) Option {
	return func(c *Config) { c.Workers = n }
}

// WithHandler sets the job handler. It is required.
func WithHandler(h queue.Handler) Option {
	return func(c *Config) { c.Handler = h }
}

// WithLimiter installs a rate limiter applied before each job starts.
func WithLimiter(l ratelimit.Limiter) Option {
	return func(c *Config) { c.Limiter = l }
}

// WithDrainTimeout bounds graceful shutdown.
func WithDrainTimeout(d time.Duration) Option {
	return func(c *Config) { c.DrainTimeout = d }
}

// WithLogger installs a structured logger.
func WithLogger(l *slog.Logger) Option {
	return func(c *Config) { c.Logger = l }
}

// WithOnResult installs a per-job callback.
func WithOnResult(fn func(Result)) Option {
	return func(c *Config) { c.OnResult = fn }
}

// New creates a Pool. Call Start to begin draining the queue.
func New(sink Sink, opts ...Option) *Pool {
	cfg := Config{Workers: 1}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	// A nil sink or handler is a programmer error. Substituting a no-op handler
	// would make every job succeed without doing any work, which is silent data
	// loss; failing loudly at construction is the only safe option.
	if sink == nil {
		panic("worker: New requires a non-nil Sink")
	}
	if cfg.Handler == nil {
		panic("worker: New requires a handler (use WithHandler)")
	}

	return &Pool{
		cfg:  cfg,
		sink: sink,
		jobs: make(chan queue.Delivery, cfg.Workers*2),
	}
}

// Start launches the workers and the dispatcher. It returns ErrAlreadyStarted
// if called twice, ErrPoolClosed if the pool has been shut down, and
// ErrNilContext if ctx is nil.
//
// Two contexts come out of this call, and the distinction matters:
//
//   - The dispatch context is derived from ctx and cancelled by Shutdown. It
//     gates Dequeue and rate-limiter waits, so Shutdown can always unblock a
//     dispatcher parked on an idle queue.
//   - The handler context is ctx itself. A graceful Shutdown deliberately does
//     NOT cancel it, so jobs already running get to finish; only cancelling ctx
//     itself (the caller's own shutdown) aborts them.
func (p *Pool) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if p.stopped.Load() {
		return ErrPoolClosed
	}
	if !p.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}

	dispatchCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	for range p.cfg.Workers {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.runWorker(ctx, dispatchCtx)
		}()
	}

	p.dispWG.Add(1)
	go func() {
		defer p.dispWG.Done()
		p.dispatch(dispatchCtx)
	}()

	return nil
}

// dispatch pulls deliveries from the queue and hands them to workers. It exits
// when the dispatch context is done, the pool is stopped, or the queue closes.
func (p *Pool) dispatch(ctx context.Context) {
	defer close(p.jobs) // signals workers that no more work is coming

	for {
		if p.stopped.Load() {
			return
		}

		d, err := p.sink.Dequeue(ctx)
		if err != nil {
			switch {
			case errors.Is(err, queue.ErrClosed):
				p.log().Info("queue closed, dispatcher stopping")
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				// Expected during shutdown.
			default:
				p.log().Error("dequeue failed", "err", err)
			}
			return
		}

		select {
		case p.jobs <- d:
		case <-ctx.Done():
			// The job is not dropped: it stays reserved in the queue and is
			// redelivered once its visibility timeout lapses, so the delivery
			// handle is simply abandoned here.
			return
		}
	}
}

// runWorker drains the job channel until it closes. handlerCtx is used for the
// handler so an in-flight job survives a graceful shutdown; throttleCtx is used
// for the rate limiter so a parked worker unwinds immediately.
func (p *Pool) runWorker(handlerCtx, throttleCtx context.Context) {
	for d := range p.jobs {
		p.execute(handlerCtx, throttleCtx, d)
	}
}

// execute applies the rate limiter, runs the handler, and reports the outcome
// back to the queue.
func (p *Pool) execute(handlerCtx, throttleCtx context.Context, d queue.Delivery) {
	if p.cfg.Limiter != nil {
		if err := p.cfg.Limiter.Wait(throttleCtx); err != nil {
			// We were cancelled or throttled out of existence. Do not consume
			// the delivery: let the reservation lapse so it is retried.
			p.log().Debug("limiter wait aborted", "job", d.Job.ID, "err", err)
			return
		}
	}

	start := time.Now()
	err := p.safeHandle(handlerCtx, d)
	res := Result{
		ID:       d.Job.ID,
		Attempt:  d.Attempt,
		Duration: time.Since(start),
		Err:      err,
	}

	p.processed.Add(1)

	switch {
	case err == nil:
		if ackErr := p.sink.Ack(d); ackErr != nil {
			// The reservation lapsed while we worked, so the queue will deliver
			// this job again. Report it as a failure: the handler succeeded but
			// the job did not, and callers must not count it as a success.
			p.failed.Add(1)
			res.Err = ackErr
			p.log().Warn("ack failed, job will be redelivered", "job", d.Job.ID, "err", ackErr)
		} else {
			p.succeeded.Add(1)
		}

	case errors.Is(err, context.Canceled) && handlerCtx.Err() != nil:
		// The pool is shutting down mid-job. Nack so the job is eligible for
		// retry, then let the reservation/nack path decide its fate. The
		// outcomes are interpreted exactly as in the default branch below:
		// ErrRetryScheduled is a success, not a failure, and logging it as one
		// would make an ordinary graceful shutdown look alarming.
		p.failed.Add(1)
		res.Err = fmt.Errorf("handler interrupted by shutdown: %w", err)
		p.applyNack(&res, d, res.Err, p.sink.Nack(d, res.Err))

	default:
		p.failed.Add(1)
		p.applyNack(&res, d, err, p.sink.Nack(d, err))
	}

	if p.cfg.OnResult != nil {
		p.safeNotify(res, d)
	}
}

// applyNack records what the queue decided about a failed job, mutates the
// result the caller reports, and updates the pool's counters. A nil nackErr
// means the cause was an unknown or already-lapsed reservation, which is
// possible but not actionable.
func (p *Pool) applyNack(res *Result, d queue.Delivery, cause, nackErr error) {
	switch {
	case errors.Is(nackErr, queue.ErrRetryScheduled):
		res.RetryScheduled = true
		p.retried.Add(1)
		p.log().Debug("job retry scheduled", "job", d.Job.ID,
			"attempt", d.Attempt, "err", cause)
	case errors.Is(nackErr, queue.ErrJobFailed):
		res.DeadLettered = true
		p.dead.Add(1)
		p.log().Error("job dead-lettered", "job", d.Job.ID,
			"attempt", d.Attempt, "err", cause)
	case errors.Is(nackErr, queue.ErrUnknownJob):
		// The visibility timeout lapsed before the worker reported back, so
		// the job is already ready again. Nothing to do but say so.
		p.log().Warn("nack lost a lapsed reservation", "job", d.Job.ID, "err", nackErr)
	case nackErr != nil:
		p.log().Error("nack failed", "job", d.Job.ID, "err", nackErr)
	}
}

// safeNotify invokes the OnResult callback, containing a panic. The callback is
// observational: a bug in it must not cost us the worker goroutine, because
// that would silently reduce the pool's capacity for the rest of its life.
func (p *Pool) safeNotify(res Result, d queue.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			p.log().Error("OnResult callback panicked", "job", d.Job.ID, "panic", r)
		}
	}()
	p.cfg.OnResult(res)
}

// safeHandle runs the handler, converting a panic into an error so one bad job
// cannot take down the whole pool. A panicking handler is a bug, but the pool's
// job is to survive it and keep the other workers busy.
func (p *Pool) safeHandle(ctx context.Context, d queue.Delivery) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w on job %q: %v", ErrHandlerPanic, d.Job.ID, r)
		}
	}()
	return p.cfg.Handler(ctx, d)
}

// Shutdown stops accepting new work and waits for in-flight jobs to finish.
// The in-flight jobs get to complete because the workers keep draining p.jobs
// while the dispatcher has already stopped feeding it.
//
// Shutdown is idempotent. If DrainTimeout is set and elapses, Shutdown returns
// context.DeadlineExceeded while the remaining jobs are abandoned to the queue's
// visibility timeout.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.once.Do(func() {
		p.stopped.Store(true)
		// Cancel the dispatch context so a dispatcher blocked inside Dequeue (or
		// a worker parked in Limiter.Wait) unwinds promptly. Without this,
		// Shutdown would wait on an idle queue until its own context expired.
		if p.cancel != nil {
			p.cancel()
		}
	})

	// Wait for the dispatcher to stop pulling. It owns cancel for poolCtx, so
	// waiting on it ensures no new jobs enter the channel.
	dispDone := make(chan struct{})
	go func() {
		p.dispWG.Wait()
		close(dispDone)
	}()

	select {
	case <-dispDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	// The dispatcher closed p.jobs, so workers drain what remains and exit.
	workersDone := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(workersDone)
	}()

	var timer <-chan time.Time
	if p.cfg.DrainTimeout > 0 {
		t := time.NewTimer(p.cfg.DrainTimeout)
		defer t.Stop()
		timer = t.C
	}

	select {
	case <-workersDone:
		p.log().Info("pool shut down cleanly",
			"processed", p.processed.Load(),
			"succeeded", p.succeeded.Load(),
			"failed", p.failed.Load())
		return nil
	case <-timer:
		// Wrap context.DeadlineExceeded so callers can treat a drain timeout
		// uniformly with any other deadline, including via errors.Is.
		return fmt.Errorf("worker: drain timed out after %s with %d jobs in flight: %w",
			p.cfg.DrainTimeout, p.InFlight(), context.DeadlineExceeded)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// InFlight reports how many jobs are currently queued for workers but not yet
// finished. It is an approximation, useful for shutdown diagnostics.
func (p *Pool) InFlight() int { return len(p.jobs) }

// PoolStats is a snapshot of pool counters.
type PoolStats struct {
	Workers   int    `json:"workers"`
	Processed uint64 `json:"processed"`
	Succeeded uint64 `json:"succeeded"`
	Failed    uint64 `json:"failed"`
	Retried   uint64 `json:"retried"`
	Dead      uint64 `json:"dead"`
	InFlight  int    `json:"in_flight"`
	Stopped   bool   `json:"stopped"`
}

// Stats returns a snapshot of pool counters.
func (p *Pool) Stats() PoolStats {
	return PoolStats{
		Workers:   p.cfg.Workers,
		Processed: p.processed.Load(),
		Succeeded: p.succeeded.Load(),
		Failed:    p.failed.Load(),
		Retried:   p.retried.Load(),
		Dead:      p.dead.Load(),
		InFlight:  p.InFlight(),
		Stopped:   p.stopped.Load(),
	}
}

func (p *Pool) log() *slog.Logger {
	if p.cfg.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return p.cfg.Logger
}
