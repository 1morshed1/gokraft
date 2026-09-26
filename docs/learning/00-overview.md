# 00 — Overview: what we are building and why

> This is the first learning doc. Read it top to bottom, then follow the
> numbered docs. Each one maps to a phase of the build and explains the
> reasoning, the trade-offs, and the Go concepts involved.

## What is "observability"?

When you run software in production, things break in ways you cannot predict.
Observability is the practice of instrumenting your systems so that, *after* a
problem happens, you can ask arbitrary questions and get answers — without
shipping new code. It rests on three "pillars", or **signals**:

- **Logs** — timestamped text records of discrete events. "User 42 failed
  login", "cache miss for key X". High volume, mostly append-only, you search
  them by time range and keywords.
- **Metrics** — numeric measurements sampled over time. "CPU is at 73%",
  "1,204 requests in the last minute". They are aggregatable: you almost never
  care about a single sample, you care about the average/max/percentile over a
  window.
- **Traces** — the story of a single request as it fans out across many
  services. A trace is a tree of **spans**, each span being one unit of work
  ("HTTP GET /cart", "SELECT from orders"). This is how you find *where* in a
  distributed system the latency or error came from.

Datadog, Grafana Cloud, Honeycomb, and the Elastic Stack are all commercial
platforms that ingest these three signals and let you query and visualize them.
gokraft is a miniature version of that, built so we understand what happens
under the hood instead of treating it as magic.

## Why build it instead of using Prometheus + Grafana?

Because *using* the tools teaches you the tools; *building* one teaches you the
problems the tools solve:

- extreme **write-heavy** workloads (tens of thousands of events per second),
- **columnar storage** and **time-series compression**,
- the difference between storage optimized for *writes* vs *reads*,
- **backpressure** and buffering (what happens when the database is slower than
  the incoming firehose),
- **query optimization** across different data shapes.

These are problems most application engineers never touch directly. That is
exactly why they are worth learning.

## The pipeline, stage by stage

```
 apps ─OTLP/gRPC─▶ Collector ─▶ Kafka ─▶ Writers ─▶ Databases ─▶ Query API ─▶ Dashboard
```

1. **Instrumented apps** emit telemetry using the OpenTelemetry SDK. They speak
   OTLP (OpenTelemetry Protocol) over gRPC. We do not control the apps; we only
   have to *accept* what the standard emits. (Our `loadgen` binary plays the
   role of these apps.)

2. **The Collector** is a gRPC server implementing the OTLP receiver interface.
   It accepts all three signals, enriches them with Kubernetes metadata (which
   pod/namespace/node sent this?), batches them, and produces them to Kafka. It
   does *not* talk to databases — that is deliberate. See doc 01.

3. **Kafka** is a durable buffer that **decouples ingestion from storage**. The
   collector can accept a burst of 100K events/sec even if the databases are
   momentarily slow; the events queue up in Kafka and the writers drain them at
   their own pace. Three topics (`logs`, `metrics`, `traces`) let each signal
   scale independently. See doc 01's "Why Kafka in the middle?" section.

4. **Writers** are three small consumer programs, one per signal. Each reads its
   topic and bulk-inserts into the matching database. Keeping them separate
   means a slow ClickHouse cannot stall metric ingestion. See doc 02.

5. **Databases** — one per signal, each chosen for that signal's access
   pattern. This is the heart of the project. See doc 02.

6. **The Query API** (Gin) translates user queries into optimized SQL for each
   backend, handles **cross-signal correlation** (jump from an error log to its
   trace), and serves a WebSocket **live tail**. See doc 03.

7. **The Alerting engine** runs inside the query API process, periodically
   evaluating rules like "error rate > 5% for 5 minutes" and firing
   notifications. See doc 04.

8. **The Dashboard** is deliberately plain (HTML + Chart.js). The backend is the
   showcase. See doc 05 (in the infra/dashboard commit).

## The one design decision to internalize

Everything downstream of the collector speaks our **normalized model**
(`internal/model`), not OTLP. We convert OTLP → model exactly once, at the
collector's edge. This decouples the whole system from the wire format and makes
every writer, query, and test dramatically simpler. If you remember one thing
from this codebase, make it this: **translate foreign formats to your own model
at the boundary, and let the rest of your system speak its own language.**

## How to read the code

- `internal/model/model.go` — the shared vocabulary. Read this first; the rest
  of the code is just moving these structs around and persisting them.
- `internal/config` and `internal/logging` — boring but load-bearing. Every
  binary uses them. 12-factor config, structured JSON logs.
- Then follow the phase docs, opening the packages they reference.

Next: [`01-collector.md`](01-collector.md).
