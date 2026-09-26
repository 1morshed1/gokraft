# ADR-0004 — Structured alert rules instead of a query DSL

**Status:** Accepted

## Context
Real alerting products accept an expression language
(`avg(rate(errors[5m])) > 0.05`). Parsing such a DSL is a substantial
sub-project and would obscure the part that actually teaches something: the
fire/resolve state machine.

## Decision
Represent a rule as a struct of typed fields (metric, service, aggregation,
comparator, threshold, window, for-duration, severity, notify-url). Evaluate it
against `MetricStore.Scalar`.

## Consequences
- **+** The alerting *logic* (ok → pending → firing, spike suppression,
  edge-triggered notifications) is front and center and fully unit-tested with
  an injected clock.
- **+** No parser to write or secure.
- **−** Less expressive than a DSL (no arithmetic across metrics, no functions).
  Adding an expression front-end is a clean, documented extension point.
