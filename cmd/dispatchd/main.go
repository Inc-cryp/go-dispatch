// Command dispatchd is a small demo service that wires the dispatch packages
// together: a bounded worker pool draining a priority queue, throttled by a rate
// limiter, with every state transition mirrored onto an event bus and exposed
// over HTTP.
//
// It is deliberately a single process with an in-memory queue. That is enough to
// exercise the interesting parts — backpressure, retries with backoff,
// dead-lettering, visibility timeouts, graceful shutdown — without pretending to
// be a distributed system. The queue interface is the seam a real backend (SQS,
// Redis, Postgres) would slot into.
//
// Usage:
//
//	dispatchd -workers 8 -rate 50 -addr :8080 -subjects 5000
//
// Endpoints:
//
//	GET /healthz  liveness; 200 while the queue is open
//	GET /stats    JSON snapshot of queue, pool, and limiter counters
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Inc-cryp/go-dispatch/eventbus"
	"github.com/Inc-cryp/go-dispatch/queue"
	"github.com/Inc-cryp/go-dispatch/ratelimit"
	"github.com/Inc-cryp/go-dispatch/worker"
)

// Job kinds produced by the demo generator. Each maps to a topic on the bus and
// a handling function, which is how a real service would route work.
const (
	kindEmailSend   = "email.send"
	kindImageResize = "image.resize"
	kindFlakyReport = "flaky.report"
	kindSlowIndex   = "slow.index"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "dispatchd: %v\n", err)
		os.Exit(1)
	}
}

type config struct {
	workers     int
	rate        float64
	burst       int
	addr        string
	subjects    int
	shutdownFor time.Duration
	logLevel    string
	// healthcheck, when non-empty, makes the process probe that URL and exit
	// 0/1 instead of starting the service. It exists because the container
	// image is FROM scratch: there is no wget or curl inside it, so the binary
	// has to be its own health probe.
	healthcheck string
}

func parseFlags(args []string) config {
	fs := flag.NewFlagSet("dispatchd", flag.ContinueOnError)
	cfg := config{}
	fs.IntVar(&cfg.workers, "workers", 8, "number of concurrent job runners")
	fs.Float64Var(&cfg.rate, "rate", 200, "maximum job starts per second (0 disables the limiter)")
	fs.IntVar(&cfg.burst, "burst", 50, "rate-limiter burst capacity in jobs")
	fs.StringVar(&cfg.addr, "addr", ":8080", "HTTP listen address for /healthz and /stats")
	fs.IntVar(&cfg.subjects, "subjects", 2000, "number of jobs to generate at startup")
	fs.DurationVar(&cfg.shutdownFor, "shutdown-timeout", 10*time.Second, "graceful shutdown deadline")
	fs.StringVar(&cfg.logLevel, "log-level", "info", "log level: debug, info, warn, error")
	fs.StringVar(&cfg.healthcheck, "healthcheck", "", "probe this URL and exit 0/1 instead of serving (for container health checks)")
	if err := fs.Parse(args); err != nil {
		// flag already printed the error and usage.
		os.Exit(2)
	}
	if cfg.workers < 1 {
		cfg.workers = 1
	}
	if cfg.subjects < 0 {
		cfg.subjects = 0
	}
	return cfg
}

func run() error {
	cfg := parseFlags(os.Args[1:])

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: parseLevel(cfg.logLevel),
	}))
	slog.SetDefault(logger)

	// Health-probe mode short-circuits everything: the process fetches the URL
	// and exits, so the container runtime gets an exit code instead of a
	// long-running service. This is the only way to probe a scratch image.
	if cfg.healthcheck != "" {
		return probeHealth(cfg.healthcheck, logger)
	}

	// The root context is cancelled on SIGINT/SIGTERM, which is what turns the
	// whole shutdown story into a single mechanism: cancelling this context
	// unblocks the dispatcher and ends the service loop in serve.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. Event bus: the observability spine. Every queue transition is mirrored
	//    onto it, so subscribers stay decoupled from the queue's internals.
	bus := eventbus.New(
		eventbus.WithBuffer(512),
		eventbus.WithWorkers(2),
		eventbus.WithErrorHandler(func(e eventbus.Event, err error) {
			logger.Error("event handler failed", "topic", e.Topic, "err", err)
		}),
	)
	defer func() {
		// The bus is also closed explicitly at the end of run; Close is
		// idempotent, so this is purely the early-return safety net. A failure
		// here is worth a line in the log but cannot change the exit status,
		// which is why it is not propagated.
		if err := bus.Close(); err != nil {
			logger.Error("closing event bus", "err", err)
		}
	}()

	// 2. Rate limiter: a library-wide quota, not a per-job-type one. A nil
	//    limiter means unthrottled, so honour -rate 0 by leaving it nil.
	var limiter ratelimit.Limiter
	if cfg.rate > 0 {
		limiter = ratelimit.NewTokenBucket(cfg.rate, cfg.burst)
	}

	// 3. Queue: delayed delivery, retries with backoff, and a dead-letter list.
	q := queue.New(
		queue.WithVisibility(5*time.Second),
		queue.WithEventBuffer(1024),
		queue.WithErrorHandler(func(err error) {
			logger.Warn("queue reported an error", "err", err)
		}),
	)
	defer func() {
		if err := q.Close(); err != nil {
			logger.Error("closing queue", "err", err)
		}
	}()

	registerObservers(ctx, bus, q, logger)

	// 4. Pool: the concurrency core, with the routing table as its handler.
	pool := worker.New(q,
		worker.WithWorkers(cfg.workers),
		worker.WithLimiter(limiter),
		worker.WithDrainTimeout(cfg.shutdownFor),
		worker.WithLogger(logger),
		worker.WithHandler(routingHandler(logger)),
		worker.WithOnResult(func(r worker.Result) {
			switch {
			case r.OK():
				logger.Debug("job ok", "job", r.ID, "attempt", r.Attempt, "took", r.Duration)
			case r.DeadLettered:
				logger.Warn("job dead-lettered", "job", r.ID, "err", r.Err)
			case r.RetryScheduled:
				logger.Info("job retry scheduled", "job", r.ID, "err", r.Err)
			default:
				// Nothing failed to route: a limiter cancellation or a lost
				// reservation lands here.
				logger.Warn("job attempt inconclusive", "job", r.ID, "err", r.Err)
			}
		}),
	)

	// 5. HTTP: health and stats, served off a separate listener so the demo can
	//    be probed while it drains.
	srv := newServer(cfg.addr, q, pool, logger)

	if err := serve(ctx, cfg, q, pool, srv, logger); err != nil {
		return err
	}

	reportFinal(q, pool, limiter)

	if err := bus.Close(); err != nil {
		return fmt.Errorf("closing event bus: %w", err)
	}
	return nil
}

// serve runs the service loop: it starts the pool, submits the workload, blocks
// until the operator signals or the listener dies, and then shuts everything
// down in order. It is separated from run so the shutdown wiring — the part that
// decides whether a SIGTERM drains the pool or guts it — can be tested without
// binding a port or sending a real signal.
//
// ctx is the dispatch context: cancelling it is what signals a shutdown.
// handlerCtx is deliberately a different context and is what the pool is
// started with. The pool derives its own cancellable dispatch context from
// whatever it is given and cancels only that derived context on Shutdown, so
// handing it ctx here would make Shutdown cancel the very handlers it is about
// to wait for: the pool would drain nothing and every in-flight job would come
// back as "handler interrupted by shutdown".
func serve(
	ctx context.Context,
	cfg config,
	q *queue.Queue,
	pool *worker.Pool,
	srv *http.Server,
	logger *slog.Logger,
) error {
	// handlerCtx is deliberately derived from ctx with WithoutCancel: it has to
	// outlive the signal that ends the service loop, but it is still tied to the
	// same request-scoped values. abortHandlers is the override for a handler
	// that refuses to finish on its own; drainCtx below bounds it in practice.
	handlerCtx, abortHandlers := context.WithCancel(context.WithoutCancel(ctx))
	defer abortHandlers()

	srvErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
			return
		}
		srvErr <- nil
	}()
	logger.Info("http listening", "addr", cfg.addr)

	if err := pool.Start(handlerCtx); err != nil {
		return fmt.Errorf("starting worker pool: %w", err)
	}

	produced := produce(ctx, q, cfg.subjects, logger)
	logger.Info("workload submitted", "jobs", produced)

	// Wait for operator signal, a listener failure, or natural completion.
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-srvErr:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
	}

	// Shutdown order matters: drainCtx bounds the whole sequence, so the
	// process cannot outlive -shutdown-timeout even if a handler ignores
	drainCtx, cancelDrain := context.WithTimeout(context.WithoutCancel(ctx), cfg.shutdownFor)
	defer cancelDrain()

	// Each stage gets its own sub-deadline, so a hung listener or overrunning
	// handlers cannot starve the stage after it.
	srvCtx, cancelSrv := context.WithTimeout(drainCtx, cfg.shutdownFor/2)
	defer cancelSrv()
	_ = srv.Shutdown(srvCtx)

	// pool.Shutdown stops the dispatcher and then waits for the workers to
	// finish the deliveries already in the channel. Those handlers run under
	// handlerCtx, which is still live here: that is what makes this a drain
	// rather than a race. Only if it overruns do we abort the handlers.
	if err := pool.Shutdown(drainCtx); err != nil {
		// A drain timeout is expected when a handler misbehaves. Tear the
		// remaining handlers down and report it, but do not fail the process:
		// the counters below tell the real story.
		abortHandlers()
		logger.Warn("pool drain did not complete cleanly", "err", err)
	}

	return nil
}

// probeHealth fetches url and reports whether the service answered 2xx. It is
// used by the container health check, which cannot shell out to curl because the
// runtime image is FROM scratch and contains no binaries but this one.
//
// The timeout is deliberately short: a health probe that blocks longer than the
// check interval is worse than one that fails fast, since the runtime would
// report the service as unhealthy either way but stack up hung probe processes.
func probeHealth(url string, logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("building health request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("health probe %s: %w", url, err)
	}
	defer resp.Body.Close()
	// Drain so the connection can be reused and the body is actually consumed
	// before the transport is torn down.
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("health probe %s: unexpected status %s", url, resp.Status)
	}
	logger.Debug("health probe ok", "url", url, "status", resp.Status)
	return nil
}

// registerObservers mirrors queue transitions onto the event bus. This is the
// bridge between the two packages, and it lives here rather than inside either
// one so neither has to know about the other.
func registerObservers(ctx context.Context, bus *eventbus.Bus, q *queue.Queue, logger *slog.Logger) {
	sub := q.Subscribe(
		queue.TopicEnqueued,
		queue.TopicDone,
		queue.TopicFailed,
		queue.TopicRetried,
		queue.TopicRequeued,
	)

	go func() {
		defer func() {
			// Closing the subscription releases the queue's event buffer for
			// this subscriber. The error is not actionable at this point in the
			// lifecycle, but it should not vanish silently either.
			if err := sub.Close(); err != nil {
				logger.Debug("closing queue subscription", "err", err)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-sub.C():
				if !ok {
					return
				}
				// Republish as a dotted topic so subscribers can use the bus's
				// pattern matching, e.g. "> "for everything or "job.failed".
				topic := "job." + ev.Topic.String()
				payload := map[string]any{
					"job_id":  ev.JobID,
					"attempt": ev.Attempt,
					"state":   ev.State.String(),
					"at":      ev.At,
				}
				if ev.Err != nil {
					payload["error"] = ev.Err.Error()
				}
				if err := bus.Publish(ctx, eventbus.Event{Topic: topic, Payload: payload}); err != nil {
					if errors.Is(err, eventbus.ErrBusClosed) {
						return
					}
					logger.Debug("publish failed", "topic", topic, "err", err)
				}
			}
		}
	}()
}

// routingHandler dispatches a job to the function matching its topic, mirroring
// how a real worker would look up a handler by job type.
//
// The handlers take the delivery but ignore most of it: what the demo needs to
// show is routing and the retry path, not payload semantics.
func routingHandler(logger *slog.Logger) queue.Handler {
	return func(ctx context.Context, d queue.Delivery) error {
		kind, _ := d.Job.Payload.(string)

		switch kind {
		case kindEmailSend:
			return handleEmail(ctx)
		case kindImageResize:
			return handleImage(ctx)
		case kindSlowIndex:
			return handleIndex(ctx)
		case kindFlakyReport:
			// Fails deterministically on the first attempt of each job, then
			// succeeds. This is what exercises the retry/backoff path without
			// making tests or demos flaky.
			if d.Attempt < 2 {
				return fmt.Errorf("upstream rate limited, attempt %d", d.Attempt)
			}
			return nil
		default:
			// An unknown job type is a permanent failure: retrying it would
			// never succeed, but the queue owns the retry policy, so report a
			// normal error and let MaxAttempts decide.
			logger.Warn("unknown job kind", "kind", kind, "job", d.Job.ID)
			return fmt.Errorf("no handler registered for kind %q", kind)
		}
	}
}

func handleEmail(ctx context.Context) error {
	// Simulate an I/O-bound call that respects cancellation.
	return sleepCtx(ctx, 2*time.Millisecond)
}

func handleImage(ctx context.Context) error {
	// Image work is the slow path, which is what makes the pool's concurrency
	// visible in the stats.
	return sleepCtx(ctx, 15*time.Millisecond)
}

func handleIndex(ctx context.Context) error {
	return sleepCtx(ctx, 40*time.Millisecond)
}

// sleepCtx sleeps unless the context is cancelled first, so shutdown does not
// wait out every simulated delay.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// produce generates a mixed workload. It returns how many jobs were accepted.
func produce(ctx context.Context, q *queue.Queue, n int, logger *slog.Logger) int {
	kinds := []struct {
		kind     string
		priority int
		attempts int
		backoff  time.Duration
		weight   int
	}{
		{kindEmailSend, 5, 3, 0, 5},
		{kindImageResize, 3, 3, 0, 3},
		{kindFlakyReport, 1, 4, 20 * time.Millisecond, 1},
		{kindSlowIndex, 0, 2, 0, 1},
	}

	var total int
	for i := range n {
		select {
		case <-ctx.Done():
			return total
		default:
		}

		// Weighted pick, so email dominates exactly as it would in a real inbox.
		w := rand.IntN(10)
		var chosen int
		acc := 0
		for idx, k := range kinds {
			acc += k.weight
			if w < acc {
				chosen = idx
				break
			}
		}
		k := kinds[chosen]

		entry := queue.Entry{
			ID:          fmt.Sprintf("%s-%d", k.kind, i),
			Payload:     k.kind,
			Priority:    k.priority,
			MaxAttempts: k.attempts,
			Backoff: func(int) time.Duration {
				return k.backoff
			},
		}
		if err := q.Enqueue(entry); err != nil {
			// A duplicate ID is impossible here; a closed queue means shutdown
			// started, so stop producing rather than spam the log.
			if errors.Is(err, queue.ErrClosed) {
				return total
			}
			logger.Warn("enqueue rejected", "job", entry.ID, "err", err)
			continue
		}
		total++
	}
	return total
}

// newServer builds the HTTP mux. It deliberately takes no limiter: the token
// count printed at shutdown comes from reportFinal, and /stats reports queue and
// pool counters because those are the ones that change while running.
func newServer(addr string, q *queue.Queue, pool *worker.Pool, logger *slog.Logger) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		st := q.Stats()
		code := http.StatusOK
		status := "ok"
		if st.Closed {
			code, status = http.StatusServiceUnavailable, "closing"
		}
		writeJSON(w, code, map[string]any{"status": status}, logger)
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		qs := q.Stats()
		ps := pool.Stats()

		body := map[string]any{
			"queue": map[string]any{
				"ready":       qs.Ready,
				"reserved":    qs.Reserved,
				"delayed":     qs.Delayed,
				"done":        qs.Done,
				"failed":      qs.Failed,
				"retried":     qs.Retried,
				"requeued":    qs.Requeued,
				"dead_letter": qs.DeadLetter,
				"closed":      qs.Closed,
			},
			"pool": map[string]any{
				"workers":   ps.Workers,
				"processed": ps.Processed,
				"succeeded": ps.Succeeded,
				"failed":    ps.Failed,
				"retried":   ps.Retried,
				"dead":      ps.Dead,
				"in_flight": ps.InFlight,
				"stopped":   ps.Stopped,
			},
			"dead_letters": q.DeadLetters(),
			"uptime_s":     time.Since(startTime).Seconds(),
		}
		writeJSON(w, http.StatusOK, body, logger)
	})

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

var startTime = time.Now()

func writeJSON(w http.ResponseWriter, code int, v any, logger *slog.Logger) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// The status line is already written, so only logging remains.
		logger.Warn("encoding response failed", "err", err)
	}
}

// reportFinal prints the end-of-run summary. Making the numbers visible at exit
// is the difference between a demo and a black box.
func reportFinal(q *queue.Queue, pool *worker.Pool, limiter ratelimit.Limiter) {
	qs := q.Stats()
	ps := pool.Stats()

	fmt.Println("--- final stats ---")
	fmt.Printf("queue      ready=%d reserved=%d delayed=%d\n", qs.Ready, qs.Reserved, qs.Delayed)
	fmt.Printf("queue      done=%d failed=%d retried=%d requeued=%d dead=%d\n",
		qs.Done, qs.Failed, qs.Retried, qs.Requeued, qs.DeadLetter)
	fmt.Printf("pool       processed=%d succeeded=%d failed=%d retried=%d dead=%d\n",
		ps.Processed, ps.Succeeded, ps.Failed, ps.Retried, ps.Dead)

	if dl := q.DeadLetters(); len(dl) > 0 {
		fmt.Printf("dead letters (%d): %v\n", len(dl), dl)
	}
	if tb, ok := limiter.(*ratelimit.TokenBucket); ok {
		fmt.Printf("limiter    tokens=%.1f\n", tb.Tokens())
	}
}

// parseLevel maps a flag string onto a slog level, defaulting to Info.
func parseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return l
}
