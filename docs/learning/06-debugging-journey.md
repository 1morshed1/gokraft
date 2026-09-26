# 06 — The debugging journey: four real bugs from the first end-to-end run

> The code in the earlier docs was written to be correct, but "compiles and unit
> tests pass" is not "works end to end". The first time the full stack ran on
> real infrastructure, four things broke — every one a genuine, common
> distributed-systems gotcha. This doc walks through each: the symptom, the
> diagnosis, and the fix. Debugging *is* the learning; don't skip it.

## Bug 1 — ClickHouse "Authentication failed" for the default user

**Symptom.** The log writer and query API both died at startup:

```
clickhouse ping: code: 516 ... default: Authentication failed:
password is incorrect, or there is no user with such name
```

…yet `clickhouse-client` *inside* the container worked with no password.

**Diagnosis.** The official ClickHouse image ships a `default-user.xml` that
restricts the `default` user to `127.0.0.1`/`::1` — localhost only. A client
inside the container is localhost; our writers (in other containers, or on the
host via the mapped port) arrive from a different IP and are rejected. The error
message blames the password, but the real cause is the **network ACL**.

**Fix.** Don't weaken the built-in `default` user. Add a purpose-made `gokraft`
user reachable from the cluster network with a password
(`deploy/clickhouse/users.xml`), and point the services at it via
`CLICKHOUSE_USER`/`CLICKHOUSE_PASS`.

**Lesson.** Auth errors are sometimes network-authorization errors in disguise.
When a credential "works here but not there", suspect *where* the connection
comes from, not just *what* it presents.

## Bug 2 — ClickHouse container wouldn't start: rlimit not permitted

**Symptom.**

```
OCI runtime create failed: ... error setting rlimit type 7: operation not permitted
```

**Diagnosis.** Our compose file asked for `ulimits: nofile: 262144` (ClickHouse's
production recommendation). The sandboxed Docker runtime forbids raising
rlimits, so the container never launched.

**Fix.** Drop the explicit ulimit for local dev (with a comment explaining why,
and to add it back on a real host). The recommendation is real for production;
it's just not settable everywhere.

**Lesson.** "Best practice" config can be environment-specific. A setting that's
correct on a bare-metal host can make a container un-startable in a restricted
runtime.

## Bug 3 — the Bitnami Kafka image tag vanished

**Symptom.** `docker.io/bitnami/kafka:3.7: not found`.

**Diagnosis.** Public image tags are not forever. Bitnami reorganized its
catalog and the pinned tag stopped resolving.

**Fix.** Switch to the official `apache/kafka:3.8.1` image — which also uses
plain `KAFKA_*` env vars instead of Bitnami's `KAFKA_CFG_*`, so the config keys
changed too.

**Lesson.** Pin image versions for reproducibility, but know that upstream can
still pull the rug. Prefer first-party images where they exist.

## Bug 4 (the big one) — data reached Kafka but never the databases

This one had two layers, uncovered in sequence.

**Symptom A.** The collector logged
`Unknown Topic Or Partition ... topic ... does not exist`, and all three
databases stayed empty even though the broker had
`auto.create.topics.enable=true`.

**Diagnosis A.** The Go client (`kafka-go`) does **not** request topic creation
on produce by default. Broker-side auto-create only fires for clients that ask
for it, so the very first produce to a missing topic just fails.

**Fix A.** Set `AllowAutoTopicCreation: true` on the writer (dev convenience;
in production you pre-create topics with a chosen partition count).

**Symptom B.** After fix A, the topics *were* created — but produces from the
host still failed:

```
dial tcp: lookup kafka on 8.8.8.8:53: no such host
```

**Diagnosis B.** The classic **Kafka advertised-listener** problem. A client
first hits a bootstrap address, then the broker tells it which address to use
for the real connection — the *advertised* listener. We advertised `kafka:9092`,
which resolves on the compose network but is meaningless on the host (which then
tries public DNS and fails). One advertised name cannot serve both audiences.

**Fix B.** Give the broker **two client listeners**: `PLAINTEXT` advertised as
`kafka:9092` for containers, and `PLAINTEXT_HOST` advertised as
`localhost:29092` for host processes. Containers use the first, `make run-*`
targets use the second.

**Lesson.** With Kafka, the address a client *connects to first* and the address
it's *told to use next* are different things. Almost every "works in the
container but not from my laptop" Kafka problem is an advertised-listener
mismatch.

## The payoff: verified end to end

After these fixes, a 12-second load run (10 services × 20 req/s) produced,
consistently across the pipeline:

- **ClickHouse:** ~2,270 logs, ~91 `ERROR` (the designed ~4% error rate).
- **TimescaleDB:** ~4,540 metric rows across all 10 services (2 metrics/request).
- **PostgreSQL:** ~4,520 spans across ~2,260 traces (2 spans/trace).

And `/api/v1/correlate/:trace_id` returned an error trace's full span tree joined
with the `ERROR` log sharing its `trace_id` — the cross-signal correlation
feature, working on live data.

**Meta-lesson.** Unit tests proved the *logic* (conversion, batching, the trace
tree, the alert state machine). Only an end-to-end run could shake out the
*integration* seams — auth, image tags, runtime limits, and client/broker
addressing. You need both kinds of testing; neither substitutes for the other.
