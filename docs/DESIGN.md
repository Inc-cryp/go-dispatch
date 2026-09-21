# Design notes

The README is the tour. This is the deep end: the invariants each package
maintains, why the current shape was chosen over the obvious alternative, and
where the design knowingly gives something up.

Read the README first. Nothing here is required to use the library.

---

## Table of contents

1. [The core problem](#1-the-core-problem)
2. [`queue`: ownership and time](#2-queue-ownership-and-time)
3. [`worker`: two contexts, one shutdown](#3-worker-two-contexts-one-shutdown)
4. [`ratelimit`: correctness under denial](#4-ratelimit-correctness-under-denial)
5. [`eventbus`: matching and backpressure](#5-eventbus-matching-and-backpressure)
6. [Failure taxonomy](#6-failure-taxonomy)
7. [Bugs found by the verification pass](#7-bugs-found-by-the-verification-pass)
8. [What would change for a real distributed backend](#8-what-would-change-for-a-real-distributed-backend)
9. [Testing strategy](#9-testing-strategy)

---

## 1. The core problem

A job queue is a state machine over time, and every hard bug in one comes from
two questions that look easy:

1. **Who owns a job right now?** The moment a job is handed to a worker, the
   queue has lost the ability to observe it. If the worker dies, the queue must
   eventually notice — but it cannot distinguish "still working" from "dead"
   without a lease.
2. **What happens when time passes?** A lease expiring and a scheduled retry
   becoming due are the same kind of event: a deadline in the future that
   changes a job's state. They must not be handled by two different mechanisms,
   or the ordering between them becomes unobservable.

`dispatch` answers (1) with a **visibility timeout** — a delivery is a lease with
a deadline, and the queue re-arms the job when the deadline passes — and (2) with
a **single scheduler goroutine** that owns every time-driven transition. Those
two decisions cascade into most of the rest of the design.

The alternative shape, which this codebase deliberately avoids, is a background
goroutine per job or per reservation. It is easier to write and much harder to
reason about: goroutine count becomes proportional to in-flight work, shutdown
becomes a fan-in over an unbounded set, and two timers racing to requeue the same
job produce duplicate deliveries. One scheduler makes the goroutine count a
constant and the ordering total.

---

## 2. `queue`: ownership and time

### 2.1 The scheduler is the only clock

`queue.New` starts exactly one scheduler goroutine. It is the sole writer of
delayed-delivery and visibility state. Producers and consumers take a mutex to
touch the heap, but *time* belongs to the scheduler alone.

The scheduler computes the earliest pending deadline (next delayed job's `RunAt`,
next reservation's expiry) and sleeps until then. When it wakes it drains
everything that is now due. Waking late is harmless — readiness is decided by
comparing timestamps, not by trusting the wakeup — so there is no correctness
dependence on timer accuracy. `WithPollInterval` exists as a floor for how long
the scheduler will sleep, which bounds latency for tests that want fast
turnaround.

**Why this matters:** because a single goroutine applies every time-driven
transition, there is no interleaving in which two timers race to requeue the same
job. Duplicate delivery then becomes a property of the *lease protocol* alone,
which is a much smaller surface to test.

### 2.2 Reservation, not removal

`Dequeue` does not remove a job. It records a reservation with a deadline and
returns a `Delivery`:

```
Delivery{ Job: Entry, Attempt: int, Deadline: time.Time, token }
```

`Ack` retires the job. `Nack` either schedules a retry or dead-letters it.
`Extend` pushes the deadline out. If none of those arrive before `Deadline`, the
scheduler returns the job to the ready set — **without consuming an attempt**.

That last clause is a deliberate choice. A lapsed reservation means the worker
never reported back: it may have crashed, or it may be hung, or the machine may
have been suspended. Charging an attempt for a verdict nobody delivered would let
an unlucky worker burn a job's entire retry budget without the handler ever
running to completion. So lapses are free, and only an explicit `Nack` consumes
an attempt.

The cost: a job whose handler reliably outlives its visibility timeout will be
redelivered forever. That is a real pathology, and the honest fix is a bounded
attempt counter that counts *deliveries* as well as verdicts. It is not
implemented, because the semantics get confusing quickly (what does a redelivery
of a job with `MaxAttempts: 1` mean?) and the current behavior is at least
defensible: **a job is only punished for a verdict it produced.**

### 2.3 The zero-time sentinel

Immediate jobs are normalized to a zero `RunAt`, not `time.Now()`.

This looks like a micro-optimization and is actually a correctness fix. When
every entry carried its own `time.Now()`, the ready heap's `(priority, readyAt,
seq)` ordering was decided by the timestamp for every same-priority pair, and
`seq` — the monotonic enqueue counter that exists precisely to break ties — only
ever fired when two timestamps were bit-identical. `TestPriorityOrdering` caught
this. Normalizing to a sentinel makes `seq` the actual tiebreaker, so FIFO
ordering within a priority class is guaranteed rather than probabilistic.

### 2.4 Backoff caps after jitter

```go
delay := base * (1 << (attempt - 1))   // exponential
delay += jitter(delay)                 // full jitter
if delay > max { delay = max }         // cap LAST
```

The cap is applied after the jitter, not before. Applying it first lets the
jitter push the result past the ceiling, so the documented maximum is exceeded
exactly for the late attempts where it matters most. The second real bug the
tests found. Full jitter (uniform in `[0, delay)`) rather than decorrelated
jitter is a simplicity call: at a 30-second ceiling with a handful of retries,
the thundering-herd argument for the fancier variants does not bite.

### 2.5 A single global mutex, on purpose

The queue uses one `sync.Mutex` for all state. Contention is not the bottleneck
at the scale this is designed for, and the alternative — sharded locks or
lock-free structures — multiplies the number of orderings a reviewer has to
verify. When the backend is swapped for a real store, this mutex disappears
entirely and is replaced by the store's concurrency control, so optimizing it now
would be optimizing code that is scheduled for deletion.

---

## 3. `worker`: two contexts, one shutdown

### 3.1 The split

`Start(ctx)` derives a *dispatch* context but passes the caller's original `ctx`
to handlers. This is the single most consequential decision in the package.

- The **dispatch context** gates `Dequeue` and limiter waits. Cancelling it
  unblocks a dispatcher parked on an empty queue immediately, instead of making
  shutdown wait for the poll interval or the caller's deadline.
- The **handler context** is the caller's context, untouched. A graceful
  `Shutdown` does **not** cancel it, so an in-flight job gets to finish.

Conflating them yields one of two bugs depending on which way the cancellation
propagates: either shutdown returns while handlers are still mutating state (work
lost), or shutdown blocks until every handler completes no matter how long that
takes (unbounded drain). Splitting them lets the pool stop *pulling* work while
in-flight work proceeds under the operator's own signal context — in
`cmd/dispatchd`, the `SIGTERM` context.

`Shutdown` is idempotent and bounded by `WithDrainTimeout`. On expiry it returns
an error wrapping `context.DeadlineExceeded`, and abandoned jobs fall back to the
queue's visibility timeout. That is the honest outcome; the alternative is an
unbounded wait.

### 3.2 `applyNack` is the only interpreter of `Nack`'s answer

`Nack` returning `ErrRetryScheduled` means **the retry was scheduled** — a
success. Two call sites nack: the ordinary failure path and the
interrupted-by-shutdown path. When each interpreted the result independently,
the shutdown path logged `ErrRetryScheduled` as `nack during shutdown failed`,
making every healthy graceful shutdown look like data loss at exactly the moment
an operator is watching.

`applyNack` now centralizes the interpretation: `ErrRetryScheduled` →
`RetryScheduled` + `retried` counter + debug log; `ErrJobFailed` → `DeadLettered`
+ `dead` counter + error log; `ErrUnknownJob` → a warn that the reservation
lapsed; anything else → an error. Both paths call it, so they cannot drift.

### 3.3 Failure accounting, stated narrowly

A job counts as **succeeded** only when the handler returned nil *and* the `Ack`
was accepted. A failed `Ack` means the reservation lapsed and the job will be
redelivered; counting it as a success would overstate throughput on exactly the
runs where the worker was too slow. `Processed` counts verdicts, not deliveries,
so a redelivered job is counted twice — which is why `Succeeded`, `Failed`,
`Retried`, and `Dead` are exposed separately rather than collapsed.

### 3.4 Panics are contained

`safeHandle` converts a handler panic into `ErrHandlerPanic` wrapping the job ID,
so the job is nacked and retried like any other failure. `safeNotify` extends the
same containment to `OnResult`. The pool's contract is to survive a panicking
handler: losing the goroutine would silently and permanently reduce capacity.

`New` panics on a nil `Sink` or nil handler. A nil handler silently replaced by a
no-op is the worst possible default — every job succeeds while doing nothing —
and that is a programming error, so it fails at construction.

---

## 4. `ratelimit`: correctness under denial

### 4.1 Lazy refill, no goroutines

A token bucket refills by arithmetic:

```go
elapsed := now.Sub(b.last)
b.tokens = min(b.burst, b.tokens + elapsed.Seconds()*b.rate)
```

No background goroutine, no per-limiter timer. Cost is O(1) per call and
proportional to the number of limiters, not to tokens or wall-clock time. A
process holding ten thousand mostly-idle limiters pays nothing for them, which
would be false for a ticker-per-bucket design. Time comes from `WithClock`, so
every refill is deterministic in tests; nothing sleeps except `Wait`.

### 4.2 `Wait` checks the context before consuming

`Wait` on an already-cancelled context returns `ctx.Err()` **without consuming a
token**. This was a fail-open bug found by the verification pass: the original
loop reserved first and checked the context second, so a cancelled caller burned
capacity it would never use, silently shrinking the effective rate for everyone
else under load.

Fixing it introduced a second bug — a `d := l.Reserve()` shadowing the outer
reservation, making the loop re-reserve on every pass. Both are pinned by tests,
because both only appear under contention.

### 4.3 `Keyed` is not a `Limiter`, deliberately

`TokenBucket`, `FixedWindow`, and `Multi` implement `Limiter` — no key, one
budget. `Keyed` requires a key, so its methods are `Allow(key)`,
`Reserve(key)`, and `Wait(ctx, key)`, and it does **not** implement `Limiter`.

If it did, an unkeyed call would have to fail open (allow everything) or fail
closed (deny everything). Both are silent catastrophes for a rate limiter, so the
API makes the unkeyed call a **compile** error instead. `TestKeyedIsNotALimiter`
asserts the type relationship that keeps this true.

`Keyed` bounds its key space (`maxKeys`) with eviction, so an attacker supplying
fresh keys cannot grow memory without limit. That is a real trade-off: evicting a
key resets its budget, so an attacker who can rotate keys *and* trigger eviction
gets a partial bypass. A production system would evict by LRU with a minimum
residency. Documented, not hidden.

### 4.4 Invalid arguments are clamped

A negative rate, a zero burst, a non-positive window — each is clamped to a sane
minimum rather than panicking. Configuration arriving from flags or the
environment should not be able to crash the process at construction. `Multi`
compounds this: `Reserve` returns the **maximum** of its children's waits,
because every child must be satisfied.

---

## 5. `eventbus`: matching and backpressure

### 5.1 Matching semantics

Patterns are dot-separated with `*` (exactly one segment) and `>` (one or more
remaining segments, legal only as the final segment). `job.>` matches
`job.created` but not the bare topic `job` — "one or more" rather than "zero or
more", because a bare-topic match is almost never what the author meant and
silently subscribing to more than intended is worse than matching nothing.

### 5.2 `Publish` is synchronous; handlers are not

`Publish` matches subscriptions and delivers to each subscriber's buffered
channel *before returning*. Handler execution happens on the subscriber's own
worker goroutines. `PublishSync` waits for handlers, for callers whose next
statement depends on the event having been observed.

`matching()` snapshots the subscriber set under the bus lock and **releases it
before delivering**. Under `DropPolicy: Block` a channel send can wait
indefinitely, and holding the bus lock across it would let one slow subscriber
stall every publisher in the process. The snapshot is safe because removal is
coordinated through the subscriber's own lock.

### 5.3 The send-on-closed-channel problem

The classic Go footgun, resolved with three pieces of state:

```go
sendMu  sync.RWMutex
closed  bool
closing chan struct{}
```

`Subscription.Close` closes `s.closing`, removes the subscription from the bus,
then takes `sendMu.Lock()` and closes `s.ch` exactly once. A publisher holds
`sendMu.RLock()` across its send, so it either completes before the closer takes
the write lock or observes `closing` closed and gives up. No send ever races a
close.

Bus `Close` is **drain-then-stop**: it closes subscriptions, letting buffered
events reach handlers before their workers exit. `Subscribe` on a closed bus
returns an already-closed subscription plus `ErrBusClosed`, so no caller ends up
holding a live-looking handle on a dead bus.

### 5.4 `DropNewest` is the default, and that is the point

The default drop policy discards the **incoming** event when a subscriber's
buffer is full, rather than blocking the publisher. This is the opposite of what
"reliable bus" usually implies, and it is deliberate: the bus is observational
infrastructure sitting next to a queue that already provides the durable,
retryable path. Blocking a producer on a logging subscriber is the worse failure.
`WithDropPolicy(Block)` is available where backpressure is wanted, and `Dropped()`
surfaces the count either way.

**The rule that follows:** if an event must not be lost, it belongs in the queue,
not on the bus.

---

## 6. Failure taxonomy

Every package distinguishes "the caller did something wrong" from "the world did
something wrong," because the responses differ.

| Sentinel | Class | Meaning |
| --- | --- | --- |
| `queue.ErrClosed` | caller | The queue was closed; stop calling it |
| `queue.ErrInvalidEntry` | caller | Missing ID or invalid field |
| `queue.ErrUnknownJob` | world | Reservation lapsed; the job already moved on |
| `queue.ErrRetryScheduled` | **success** | `Nack` scheduled a retry |
| `queue.ErrJobFailed` | terminal | Attempts exhausted; dead-lettered |
| `worker.ErrAlreadyStarted` | caller | `Start` called twice |
| `worker.ErrPoolClosed` | caller | `Start` after `Shutdown` |
| `worker.ErrNilContext` | caller | `Start(nil)` |
| `worker.ErrHandlerPanic` | world | Handler panicked; job nacked |
| `eventbus.ErrBusClosed` | caller | Bus closed |
| `eventbus.ErrInvalidPattern` | caller | Malformed topic pattern |
| `eventbus.ErrSubClosed` | caller | Subscription closed |

`ErrRetryScheduled` being a success value is the one that bites. Any code
treating a non-nil `Nack` return as failure is wrong, and §3.2 exists because
that mistake was made once already.

Errors are wrapped with `%w` so `errors.Is` works through layers, and the
shutdown drain error wraps `context.DeadlineExceeded` so a caller can treat it
uniformly with any other deadline.

---

## 7. Bugs found by the verification pass

Recorded because they are the evidence the tests earn their keep. Every one was
found by a test, not by reading.

| Bug | Symptom | Fix |
| --- | --- | --- |
| Priority ordering inert | `seq` never broke ties; ordering depended on clock resolution | Zero-time sentinel for immediate jobs |
| Backoff ceiling exceeded | Cap applied before jitter, so late retries overshot the max | Cap after jitter |
| `Wait` fail-open | Cancelled caller still consumed a token | Check `ctx.Err()` before and after `Allow` |
| Shadowed reservation | Loop re-reserved every iteration after the fix above | Hoist `d := l.Reserve()` out of the loop |
| `Keyed` API contradiction | Doc said keyed, stubs were unkeyed and failed open | Keyed signatures; `Limiter` deliberately not implemented |
| Misleading shutdown log | Healthy shutdown logged `nack during shutdown failed` | `applyNack` shared by both paths |
| `job` wrapper struct | Single-field struct with a dead `release func()` | Collapsed to `chan queue.Delivery` |
| Benchmark ID collisions | `time.Now().UnixNano()` produced duplicate live IDs | `atomic.Uint64` counter |
| Benchmark hang | Prefilled a fixed `1<<16` while `RunParallel` totalled `b.N` | Prefill exactly `b.N` |
| Benchmark hang (2) | Frozen clock made `Wait` block once the burst was exhausted | `benchClock.step` advances on every read |

The last one is worth generalizing: **a benchmark must not use a frozen clock
where the code under test can block.** `BenchmarkWaitImmediate` passed at
`-benchtime 50ms` (b.N ≈ burst, so it never had to wait) and hung the entire
`-bench .` run at `100ms`. A frozen clock is only safe for denial paths that
never block.

---

## 8. What would change for a real distributed backend

The interfaces were shaped by asking what an SQS/Redis/Postgres backend would
need. Concretely:

- **`queue.Backend`** behind the existing surface. Redis maps to a sorted set for
  delayed jobs plus per-job locks for reservations; Postgres maps to
  `FOR UPDATE SKIP LOCKED` plus `LISTEN/NOTIFY` for wakeups. Both change the
  scheduler from "own the clock" to "ask the store what is due," which is why the
  scheduler is isolated.
- **Fencing tokens.** `Delivery.token` is an in-process `uint64`. Across
  processes it must become a monotonic fencing token so a resurrected worker
  cannot `Ack` a job another worker now owns. This is the change that most
  affects the public API: `Ack` would need to fail loudly on a stale token rather
  than return `ErrUnknownJob`.
- **Idempotency keys.** At-least-once delivery is a contract the *consumer* must
  honour. The queue can help by exposing a dedup key, but it cannot provide
  exactly-once on its own.
- **Bounded attempts for lapsed reservations.** §2.2's "lapses are free" rule
  needs a companion counter, or a pathological job is immortal.
- **Fair scheduling.** One global priority heap starves low-priority work under
  sustained load. Per-tenant queues with weighted round-robin is the standard fix.

---

## 9. Testing strategy

Roughly half the repository is tests, and the strategy is deliberate:

- **Time is injected.** Every package that depends on wall-clock time accepts a
  clock, so backoff, refill, and visibility expiry are asserted exactly rather
  than slept through.
- **Real timers only for blocking waits**, with generous margins. `Wait` and the
  drain genuinely block, so those tests use deadlines large enough that
  scheduling jitter cannot make them flaky.
- **Every package has a goroutine-leak test.** `make leak` runs them. After the
  object under test is closed, the goroutine count must return to baseline — the
  only way to catch a scheduler or worker that was started and never joined.
- **Idempotence is asserted, not assumed.** `Close`/`Shutdown` are called
  repeatedly and after error paths, because "it is idempotent" decays the moment
  someone adds a field.
- **No test framework.** The assertion vocabulary is `t.Fatalf` with a message
  naming the expectation and the observed value. A helper that hides that is a
  liability.
- **Tests that encode a bug get a comment saying so.** The regression tests for
  the priority sentinel, the backoff cap, and the shutdown nack each explain what
  used to be wrong, so a future reader cannot "simplify" the fix away.

Benchmarks are measured, never estimated, and live beside the code they exercise
so they cannot silently rot. Every number in the README comes from a recorded run.
