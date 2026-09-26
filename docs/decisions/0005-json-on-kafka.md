# ADR-0005 — JSON (not protobuf) on the Kafka wire

**Status:** Accepted

## Context
Records flowing collector → Kafka → writers need a serialization format. The
obvious production choice is protobuf/Avro with a schema registry.

## Decision
Serialize the normalized model structs as JSON on Kafka.

## Consequences
- **+** Inspectable: you can attach a console consumer to a topic and read
  exactly what's flowing — invaluable while learning and debugging.
- **+** Zero schema tooling; Go's `encoding/json` is enough.
- **−** Larger messages and slower encode/decode than protobuf.
- **−** No enforced schema evolution.

Chosen because clarity beats raw efficiency for a teaching platform. Switching to
protobuf/Avro is localized to `internal/kafkabus` (producer + consumer) and is a
natural next step for a performance pass.
