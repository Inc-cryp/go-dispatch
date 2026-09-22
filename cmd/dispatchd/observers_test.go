package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Inc-cryp/go-dispatch/eventbus"
	"github.com/Inc-cryp/go-dispatch/queue"
)

// TestRegisterObserversBridgesEveryTopic drives registerObservers, the real
// bridge between the queue's event stream and the bus, and asserts that every
// advertised topic makes it across. It checks the wiring end to end rather than
// either component alone: a topic the queue never emits, or one the bridge
// forgets to subscribe to, shows up here as a bus event that never arrives.
//
// The dequeued transition is the one that used to be missing on both sides, so
// the test insists on it by name rather than only counting events.
func TestRegisterObserversBridgesEveryTopic(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := queue.New(
		queue.WithVisibility(time.Second),
		queue.WithEventBuffer(1024),
	)
	defer q.Close()

	var mu sync.Mutex
	seen := map[string]int{}

	if _, err := bus.Subscribe("job.>", func(_ context.Context, e eventbus.Event) error {
		mu.Lock()
		seen[e.Topic]++
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("bus.Subscribe: %v", err)
	}

	registerObservers(ctx, bus, q, discardLogger())

	jobCtx, jobCancel := context.WithTimeout(ctx, 2*time.Second)
	defer jobCancel()

	// One job acked, one dead-lettered after exhausting two attempts, and one
	// abandoned mid-flight so its reservation lapses. Together they cover every
	// transition the bridge is expected to forward.
	enqueue := func(e queue.Entry) {
		t.Helper()
		if err := q.Enqueue(e); err != nil {
			t.Fatalf("Enqueue %q: %v", e.ID, err)
		}
	}
	enqueue(queue.Entry{ID: "ok", MaxAttempts: 1})
	enqueue(queue.Entry{ID: "flaky", MaxAttempts: 2})

	// flaky fails its first attempt, which schedules a retry with backoff, so
	// the second reservation is not immediately available. Nack it twice.
	deliveries := map[string]queue.Delivery{}
	for range 2 {
		d, err := q.Dequeue(jobCtx)
		if err != nil {
			t.Fatalf("Dequeue: %v", err)
		}
		if d.Job.ID == "ok" {
			if err := q.Ack(d); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			continue
		}
		if err := q.Nack(d, errBoom); err != nil && !errors.Is(err, queue.ErrRetryScheduled) {
			t.Fatalf("Nack: %v", err)
		}
		deliveries[d.Job.ID] = d
	}

	// Abandon a reservation on purpose: the scheduler requeues it once the
	// visibility window lapses, which is the only way to produce job.requeued.
	abandoned, err := q.Dequeue(jobCtx)
	if err != nil {
		t.Fatalf("Dequeue for the job to abandon: %v", err)
	}
	if abandoned.Job.ID != "flaky" {
		t.Fatalf("abandoned %q, want %q", abandoned.Job.ID, "flaky")
	}

	// Drain flaky's scheduled retry and fail its last attempt, so job.failed
	// is produced too and no work is left behind for the queue to requeue.
	deadline := time.After(5 * time.Second)
	for !waitForTopic(t, q, &mu, seen, "job.retried") {
		select {
		case <-deadline:
			t.Fatalf("timed out; stats=%+v seen=%v", q.Stats(), snapshot(seen, &mu))
		case <-time.After(5 * time.Millisecond):
		}
	}
	retry, err := q.Dequeue(jobCtx)
	if err != nil {
		t.Fatalf("Dequeue for the retry: %v", err)
	}
	if err := q.Nack(retry, errBoom); !errors.Is(err, queue.ErrJobFailed) {
		t.Fatalf("Nack of the final attempt = %v, want ErrJobFailed", err)
	}

	// A second Dequeue would not help the abandoned job: requeueing only makes
	// it ready, and readiness is observable through Stats.
	for !waitForTopic(t, q, &mu, seen, "job.requeued") {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for the abandoned reservation to lapse; stats=%+v seen=%v",
				q.Stats(), snapshot(seen, &mu))
		case <-time.After(5 * time.Millisecond):
		}
	}

	for _, topic := range []string{
		"job.enqueued",
		"job.dequeued",
		"job.done",
		"job.retried",
		"job.failed",
		"job.requeued",
	} {
		if n := count(seen, &mu, topic); n == 0 {
			t.Errorf("the bus never saw %q", topic)
		}
	}
}

// waitForTopic reports whether the bus has observed a topic, and fails the test
// itself if the queue never even counted the matching transition, which
// distinguishes "the bridge dropped it" from "the queue never emitted it".
func waitForTopic(t *testing.T, q *queue.Queue, mu *sync.Mutex, seen map[string]int, topic string) bool {
	t.Helper()
	if n := count(seen, mu, topic); n > 0 {
		return true
	}
	stats := q.Stats()
	switch topic {
	case "job.retried":
		if stats.Retried == 0 {
			t.Fatalf("the queue never retried anything; stats=%+v", stats)
		}
	case "job.requeued":
		if stats.Requeued == 0 {
			t.Fatalf("the queue never requeued anything; stats=%+v", stats)
		}
	}
	return false
}

func count(seen map[string]int, mu *sync.Mutex, topic string) int {
	mu.Lock()
	defer mu.Unlock()
	return seen[topic]
}

func snapshot(seen map[string]int, mu *sync.Mutex) map[string]int {
	mu.Lock()
	defer mu.Unlock()
	out := make(map[string]int, len(seen))
	for k, v := range seen {
		out[k] = v
	}
	return out
}

// errBoom is the failure a handler reports to drive the retry and dead-letter
// paths.
var errBoom = errors.New("handler failed")
