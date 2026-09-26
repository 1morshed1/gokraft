# 04 — The Alerting Engine: from measurements to stable events

> Package: `internal/alert`. Runs inside the `queryapi` process.

Alerting sounds simple — "tell me when error rate exceeds 5%" — but the naive
version is useless in practice. A metric that momentarily blips above a threshold
would fire an alert, blip back, fire a resolve, and repeat, burying the on-call
engineer in noise. The real work of an alerting engine is turning a stream of
point-in-time measurements into **stable fire/resolve events**. That state
machine is the lesson of this phase.

## Two deliberate simplifications (and what the real thing does)

1. **Structured rules, not a query language.** Datadog/Prometheus let you write
   `avg(rate(errors[5m])) > 0.05`. Parsing that DSL is a whole project and would
   bury the alerting logic. So a `Rule` (`rule.go`) is a struct of typed fields:
   `Metric`, `Service`, `Aggregation`, `Comparator`, `Threshold`, `Window`
   (lookback for the aggregate), `For` (how long it must breach), `Severity`,
   `NotifyURL`. Adding an expression parser later is a clean extension point.

2. **In-memory rule store.** Rules live in a concurrency-safe `RuleStore`
   (`sync.RWMutex`) rather than a database — fine for a learning platform, where
   rules are few and reloading defaults on restart is acceptable. Persisting them
   to Postgres is a documented exercise. Note the store must be thread-safe
   because two goroutines touch it: the HTTP handlers (CRUD) and the evaluator.

## The state machine (`evaluator.go`) — the important bit

Per rule we track one of three states:

```
        breaching & For not yet elapsed
   ┌────────────────────────────────────────┐
   │                                         ▼
 [OK] ──breaching──▶ [PENDING] ──breached ≥ For──▶ [FIRING]
   ▲                     │                            │
   │                     │ clears                     │ clears
   └─────────────────────┴────────────────────────────┘
              (resolve emitted only on FIRING → OK)
```

- **OK** — not breaching.
- **PENDING** — breaching, but not yet for the full `For` duration. **No
  notification.** This is the spike suppressor.
- **FIRING** — breached continuously for at least `For`. A single "firing"
  notification is emitted on the edge into FIRING.
- On recovery we go back to OK and emit **one** "resolved" notification (only if
  we had actually fired).

The key idea: **notifications fire on state *edges*, not every tick.** Staying in
FIRING sends nothing further; the resolve is sent exactly once. That is what
turns a noisy signal into a single actionable alert.

### Why it's fully testable

The evaluator depends on three injected things, never on real infrastructure:

- a `ScalarFunc` for the metric value (TimescaleDB's `Store.Scalar` in
  production, a fake returning a controlled number in tests),
- an injectable `now func() time.Time` clock,
- a `Notifier` interface.

So `evaluator_test.go` drives fake time tick by tick and asserts the exact
sequence: no fire at t=0 or t=1m (still PENDING), exactly one fire at t=2m
(For elapsed), no re-fire at t=3m (already FIRING), exactly one resolve at t=4m
(recovered). There's a second test proving a breach that clears *before* `For`
never fires and resets cleanly. This is the payoff of designing for
dependency injection: the trickiest logic in the system is verified in
milliseconds with no Kafka, no database, no real clock.

Design note: "no data" is treated as "not breaching" — we never page someone
because a metric went silent. (A production system often *does* want a separate
"no data" alert; that's a conscious policy choice, made explicit here.)

## Notifications (`notifier.go`)

Two implementations behind the `Notifier` interface:

- **`LogNotifier`** — logs fire/resolve; the default and handy in dev.
- **`WebhookNotifier`** — POSTs a JSON payload to the rule's `NotifyURL` (a Slack
  incoming webhook, PagerDuty, an email gateway). It falls back to logging for
  rules without a URL so nothing is silently dropped, uses a short client timeout
  so a hung webhook can't stall the evaluation loop, and treats delivery as
  best-effort (a failed POST is logged, not retried forever — an undeliverable
  alert shouldn't wedge the engine).

## Wiring it up

`cmd/queryapi` builds the `RuleStore`, seeds a couple of demonstrative rules that
match the load generator's metrics, constructs the `Evaluator` with
`metricStore.Scalar` as its `ScalarFunc`, and runs `evaluator.Run(ctx, 30s)` in a
goroutine. The alert CRUD endpoints (`handlers.go`) let the dashboard manage
rules and read each rule's live state.

Next: [`05-run-and-loadtest.md`](05-run-and-loadtest.md) — running the whole
thing with docker-compose, the dashboard, and load testing.
