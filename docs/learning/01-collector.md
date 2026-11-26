# 01 — The Collector: accepting OTLP and getting it into Kafka

> Packages: `internal/collector`, `internal/enrich`, `internal/batch`,
> `internal/kafkabus`, binary `cmd/collector`.

The collector is the front door. Everything the platform knows about the outside
world enters here. Its job is narrow and it does it well: **accept OTLP, turn it
into our model, tag it with where it came from, and hand it to Kafka in
efficient batches.** It deliberately does *not* touch a database.

## Why speak OTLP?

OTLP (OpenTelemetry Protocol) is the emerging industry standard for shipping
telemetry. Every major language has an OpenTelemetry SDK that can export OTLP
over gRPC. By implementing the OTLP receiver interface, gokraft can ingest data
from *any* instrumented application with **zero custom integration** — the app
just points its exporter at our address. If we invented our own ingestion API,
every app would need a gokraft-specific shim. Standards win.

The OpenTelemetry project publishes the protobuf service definitions and
generates Go interfaces from them. We don't need `protoc`; we import the
pre-generated Go package `go.opentelemetry.io/proto/otlp/...` and implement three
server interfaces: `LogsServiceServer`, `MetricsServiceServer`,
`TraceServiceServer`.

### A Go gotcha: three services, one method name

All three OTLP services declare a method literally named `Export`. Go has no
method overloading — a single type can have only one method called `Export`. So
one struct **cannot** implement all three interfaces. The fix (in
`receiver.go`) is three tiny receiver types, one per signal. This is a good
reminder that Go's "no overloading" rule occasionally forces structural
decisions you wouldn't make in Java/C++.

## OTLP → model: flattening the tree

OTLP nests data to avoid repetition: one export request → many `ResourceLogs`
(each identifies a service) → many `ScopeLogs` (instrumentation libraries) →
many `LogRecord`s. Service name and K8s attributes live once on the *resource*,
not on every record.

`convert.go` walks that tree and emits one flat `model.Log` / `model.Metric` /
`model.Span` per record, copying resource-level context down. Highlights:

- **`service.name`** is pulled from the resource's attributes (OTLP semantic
  convention), defaulting to `unknown_service` like the real SDKs do.
- **Trace/span IDs** arrive as raw bytes; we hex-encode them to the strings
  everyone expects to join and display.
- **Timestamps** are nanoseconds since the Unix epoch (OTLP's unit everywhere).
- **Severity** is normalized to a small fixed set (INFO/WARN/ERROR/…) so the
  ClickHouse `LowCardinality(String)` column stays genuinely low-cardinality.
- **Metrics** are a `oneof` over Gauge/Sum/Histogram/…. We fully handle the two
  common number types and decompose histograms into `_count`/`_sum` companion
  series — enough to chart throughput and latency. (Full bucket handling is a
  documented exercise.)

The tests in `convert_test.go` build small OTLP requests by hand and assert the
resulting model structs — construct a couple of them yourself to feel how the
nesting works.

## Enrichment: answering "where did this come from?"

`internal/enrich` attaches Kubernetes context (pod/namespace/node). The lesson
is **dependency inversion**: the collector depends on an `Enricher` *interface*,
not a concrete implementation.

- The shipped `StaticEnricher` reads the pod's identity from environment
  variables injected by the Kubernetes **Downward API**. Simple, no extra
  permissions, no heavy dependencies.
- A future `APIServerEnricher` could resolve the *sender's* identity by querying
  the Kubernetes API for the pod with a matching IP and caching it ~60s. It
  drops in behind the same interface without changing any caller. We describe it
  but don't wire it in, to avoid pulling the large `client-go` dependency into the
  project.

## Batching: the single most important throughput lever

`internal/batch` is a generic, concurrency-safe **size-or-time batcher**. It
flushes when *either*:

- the buffer reaches `BATCH_SIZE` records (default 1,000), **or**
- `BATCH_DELAY` elapses since the batch's first record (default 500ms).

Why it matters: writing one Kafka message per log line means the per-message
overhead (network round-trips, broker bookkeeping) dwarfs the payload. Batching
1,000 records into one write cuts that overhead by ~1000×. The time trigger
bounds latency when traffic is light so a lone record never waits forever.

Two design details worth studying in `batch.go`:

1. **A single owner goroutine** holds the pending buffer and the timer. Because
   only that goroutine touches the buffer, there are no locks around it — a
   textbook application of *"share memory by communicating."* Callers push via a
   channel.
2. **Backpressure via a bounded channel.** When flush can't keep up, the intake
   channel fills and `Add` blocks. That backpressure travels all the way back to
   the OTLP client as slower `Export` calls — the correct way to signal "slow
   down" instead of silently dropping data. See the race-tested `batch_test.go`.

## Why Kafka in the middle?

`internal/kafkabus` wraps the producer and consumer. Kafka decouples the two
fundamentally different rates in the system:

- **Ingest rate** — how fast telemetry arrives (bursty, spiky, out of our
  control).
- **Persist rate** — how fast the databases accept writes (steadier, bounded).

Kafka is a durable, replayable log that absorbs the difference. Benefits:

- **Burst absorption.** A 100K-events/sec spike queues in Kafka; the writers
  drain it at their own pace instead of dropping data or blocking senders.
- **Isolation & independent scaling.** Three topics (`logs`, `metrics`,
  `traces`) mean a log flood can't starve metric ingestion, and each writer
  scales on its own by adding consumers to its group.
- **At-least-once delivery.** Writers commit their Kafka offset only *after* a
  successful database write. Crash mid-batch? The uncommitted messages are
  redelivered. The price is possible duplicates, which we neutralize with
  idempotent writes (next doc).

We serialize records as **JSON** on the wire. It's inspectable — you can attach a
console consumer to a topic and read exactly what's flowing — which is worth a
lot while learning. Production systems often switch to protobuf/Avro with a
schema registry for compactness and schema evolution; the trade-off is opacity.

## Lifecycle: losing zero telemetry on shutdown

`server.go`'s `Shutdown()` drains in a specific order:

1. `grpc.GracefulStop()` — stop accepting new requests, let in-flight ones
   finish.
2. Close the batchers — flushes buffered records to Kafka.
3. Close the Kafka producers — flushes their internal buffers.

Order matters: closing producers before batchers would leave the final flush
with nowhere to go. `cmd/collector/main.go` wires `SIGINT`/`SIGTERM` (what
Kubernetes sends on a rolling deploy, and what Ctrl-C sends locally) to this
drain.

Next: [`02-storage.md`](02-storage.md) — one database per signal, and the
writers that feed them.
