package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Inc-cryp/go-dispatch/eventbus"
	"github.com/Inc-cryp/go-dispatch/queue"
	"github.com/Inc-cryp/go-dispatch/worker"
)

// discardLogger keeps expected warnings out of the test output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
}

func TestRoutingHandlerDispatchesByKind(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	h := routingHandler(discardLogger())

	tests := []struct {
		name    string
		kind    string
		attempt int
		wantErr bool
	}{
		{name: "email", kind: kindEmailSend, attempt: 1},
		{name: "image", kind: kindImageResize, attempt: 1},
		{name: "index", kind: kindSlowIndex, attempt: 1},
		{name: "flaky first attempt fails", kind: kindFlakyReport, attempt: 1, wantErr: true},
		{name: "flaky second attempt succeeds", kind: kindFlakyReport, attempt: 2},
		{name: "unknown kind is a permanent error", kind: "nope.unknown", attempt: 1, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			d := queue.Delivery{
				Job:     queue.Entry{ID: "j1", Payload: tc.kind},
				Attempt: tc.attempt,
			}
			err := h(ctx, d)
			if tc.wantErr != (err != nil) {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestSleepCtxReturnsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := sleepCtx(ctx, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("sleepCtx blocked for %s; it must return as soon as the context is done", elapsed)
	}
}

func TestProduceGeneratesTheRequestedWorkload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := queue.New()
	defer q.Close()

	const n = 500
	if got := produce(ctx, q, n, discardLogger()); got != n {
		t.Fatalf("produce returned %d, want %d", got, n)
	}
	if got := q.Len(); got != n {
		t.Fatalf("queue length = %d, want %d", got, n)
	}

	// Every job must carry a kind the router knows, otherwise the demo would
	// silently dead-letter its own workload.
	known := map[string]bool{
		kindEmailSend:   true,
		kindImageResize: true,
		kindFlakyReport: true,
		kindSlowIndex:   true,
	}

	p, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	kind, ok := p.Job.Payload.(string)
	if !ok || !known[kind] {
		t.Fatalf("payload = %#v, want one of the registered kinds", p.Job.Payload)
	}
	if p.Job.MaxAttempts < 1 {
		t.Fatalf("MaxAttempts = %d, want at least 1", p.Job.MaxAttempts)
	}
	if err := q.Ack(p); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestProduceStopsOnClosedQueue covers the shutdown race: the generator runs
// concurrently with Shutdown, so it must treat a closed queue as a signal to
// stop rather than logging an error per remaining job.
func TestProduceStopsOnClosedQueue(t *testing.T) {
	q := queue.New()
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if got := produce(ctx, q, 1000, discardLogger()); got != 0 {
		t.Fatalf("produce accepted %d jobs into a closed queue, want 0", got)
	}
}

// TestEndToEndDrainsEverything runs the real wiring at a small scale and asserts
// the property the demo exists to show: every accepted job reaches a terminal
// state, and the flaky kind is retried before it succeeds.
func TestEndToEndDrainsEverything(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bus := eventbus.New()
	defer bus.Close()

	q := queue.New(queue.WithVisibility(5 * time.Second))
	defer q.Close()

	var retries atomic.Int64
	var succeeded atomic.Int64

	// The pool runs first, then the workload is submitted. Starting the pool on
	// an empty queue exercises the "dispatcher waits for work" path too.
	pool := worker.New(q,
		worker.WithWorkers(4),
		worker.WithHandler(routingHandler(discardLogger())),
		worker.WithOnResult(func(r worker.Result) {
			if r.RetryScheduled {
				retries.Add(1)
			}
			if r.OK() {
				succeeded.Add(1)
			}
		}),
	)
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const easy = 40
	if err := q.Enqueue(queue.Entry{
		ID: "flaky.report-0", Payload: kindFlakyReport, Priority: 1, MaxAttempts: 3,
		Backoff: func(int) time.Duration { return 10 * time.Millisecond },
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	for i := range easy {
		if err := q.Enqueue(queue.Entry{
			ID:          "email-" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
			Payload:     kindEmailSend,
			Priority:    3,
			MaxAttempts: 2,
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// Wait for the queue to reach a terminal state: every job acked.
	want := uint64(easy + 1)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && q.Stats().Done < want {
		time.Sleep(5 * time.Millisecond)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := pool.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	qs := q.Stats()
	if qs.Done != want {
		t.Fatalf("queue done = %d, want %d; stats = %+v", qs.Done, want, qs)
	}
	if qs.DeadLetter != 0 {
		t.Fatalf("dead letters = %d, want 0 (ids=%v)", qs.DeadLetter, q.DeadLetters())
	}
	if qs.Failed != 0 {
		t.Fatalf("failed = %d, want 0", qs.Failed)
	}
	if retries.Load() == 0 {
		t.Fatal("no retries observed; the flaky job should have failed at least once")
	}

	ps := pool.Stats()
	if got := succeeded.Load(); got != int64(want) {
		t.Fatalf("pool succeeded = %d, want %d", got, want)
	}
	if ps.InFlight != 0 {
		t.Fatalf("in-flight = %d after a clean shutdown, want 0", ps.InFlight)
	}
	if !ps.Stopped {
		t.Fatal("pool not marked stopped after Shutdown")
	}
}

// TestUnknownKindDeadLetters proves a job with no handler reaches the DLQ rather
// than being retried forever.
func TestUnknownKindDeadLetters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	bus := eventbus.New()
	defer bus.Close()

	q := queue.New(queue.WithVisibility(2 * time.Second))
	defer q.Close()

	var dead atomic.Int64
	pool := worker.New(q,
		worker.WithWorkers(2),
		worker.WithHandler(routingHandler(discardLogger())),
		worker.WithOnResult(func(r worker.Result) {
			if r.DeadLettered {
				dead.Add(1)
			}
		}),
	)
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := q.Enqueue(queue.Entry{
		ID: "bad-1", Payload: "no.such.kind", MaxAttempts: 2,
		Backoff: func(int) time.Duration { return 10 * time.Millisecond },
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && q.Stats().DeadLetter == 0 {
		time.Sleep(5 * time.Millisecond)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := pool.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if got := q.Stats().DeadLetter; got != 1 {
		t.Fatalf("dead letters = %d, want 1", got)
	}
	if got := dead.Load(); got != 1 {
		t.Fatalf("OnResult dead-lettered count = %d, want 1", got)
	}
	ids := q.DeadLetters()
	if len(ids) != 1 || ids[0] != "bad-1" {
		t.Fatalf("DeadLetters() = %v, want [bad-1]", ids)
	}
}

func TestWriteJSONSetsContentTypeAndBody(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, 201, map[string]int{"n": 7}, discardLogger())

	if rec.Code != 201 {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Fatalf("content type = %q", ct)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"n":7}` {
		t.Fatalf("body = %q", got)
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		in   string
		want slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"WARN", slog.LevelWarn},
		{"error", slog.LevelError},
		{"", slog.LevelInfo},
		{"nonsense", slog.LevelInfo},
	}
	for _, tc := range tests {
		if got := parseLevel(tc.in); got != tc.want {
			t.Errorf("parseLevel(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestParseFlagsClampsInvalidValues(t *testing.T) {
	cfg := parseFlags([]string{"-workers", "0", "-subjects", "-5"})
	if cfg.workers != 1 {
		t.Errorf("workers = %d, want 1", cfg.workers)
	}
	if cfg.subjects != 0 {
		t.Errorf("subjects = %d, want 0", cfg.subjects)
	}
}

// TestConcurrentProduceAndCloseIsRaceFree exercises the shutdown race the demo
// actually hits: the generator enqueues while another goroutine closes the
// queue. Run under -race, this must stay clean.
func TestConcurrentProduceAndCloseIsRaceFree(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		produce(ctx, q, 5000, discardLogger())
	}()

	time.Sleep(2 * time.Millisecond)
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()
}

// TestHealthcheckFlagIsRecorded guards the flag wiring: the container health
// check depends on -healthcheck reaching config, and an empty default must keep
// the service in normal serving mode.
func TestHealthcheckFlagIsRecorded(t *testing.T) {
	if got := parseFlags(nil).healthcheck; got != "" {
		t.Errorf("default healthcheck = %q, want empty (serving mode)", got)
	}

	const url = "http://127.0.0.1:8080/healthz"
	if got := parseFlags([]string{"-healthcheck", url}).healthcheck; got != url {
		t.Errorf("healthcheck = %q, want %q", got, url)
	}
}

// TestProbeHealthReportsStatus asserts both halves of the contract the container
// runtime depends on: a 2xx is a success, anything else is an error, and a
// server that is not listening is an error rather than a hang.
func TestProbeHealthReportsStatus(t *testing.T) {
	logger := discardLogger()

	tests := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{name: "200 ok", status: http.StatusOK, wantErr: false},
		{name: "204 no content", status: http.StatusNoContent, wantErr: false},
		{name: "500 internal error", status: http.StatusInternalServerError, wantErr: true},
		{name: "404 not found", status: http.StatusNotFound, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			err := probeHealth(srv.URL, logger)
			if tc.wantErr && err == nil {
				t.Fatalf("probeHealth(status %d) = nil, want an error", tc.status)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("probeHealth(status %d) = %v, want nil", tc.status, err)
			}
		})
	}

	// A closed listener must fail fast rather than block until the process-wide
	// probe timeout, otherwise the health check stacks up hung processes.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	start := time.Now()
	if err := probeHealth(url, logger); err == nil {
		t.Fatal("probeHealth on a closed server = nil, want an error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("probeHealth took %s to fail on a closed listener, want a fast failure", elapsed)
	}
}
