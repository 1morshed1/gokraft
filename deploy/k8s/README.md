# Kubernetes deployment

These manifests deploy the **gokraft services** (collector, three writers, query
API). They assume the four stateful dependencies — Kafka, ClickHouse,
TimescaleDB, PostgreSQL — already exist in the cluster (install those with their
official Helm charts / operators; running stateful databases well in Kubernetes
is a topic of its own and out of scope here). Connection details are injected via
the `gokraft-config` ConfigMap.

```
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/config.yaml
kubectl apply -f deploy/k8s/collector.yaml   # Deployment + Service + HPA
kubectl apply -f deploy/k8s/writers.yaml     # three Deployments
kubectl apply -f deploy/k8s/queryapi.yaml    # Deployment + Service
```

## What each piece demonstrates

- **The collector is horizontally autoscaled (HPA)** on CPU. Ingestion is the
  bursty, unpredictable part of the system, so it is the natural place for
  elastic scaling. Because the collector is stateless (it just converts and
  produces to Kafka), adding replicas is safe and linear.
- **The collector uses the Downward API** to inject `K8S_POD_NAME`,
  `K8S_NAMESPACE`, and `K8S_NODE_NAME` — exactly the environment variables the
  `StaticEnricher` reads (see `internal/enrich`). This is the enrichment path
  wired up for real.
- **Writers scale by Kafka consumer group.** Each writer Deployment can be scaled
  independently (`kubectl scale deployment/logwriter --replicas=3`); Kafka
  rebalances partitions across the replicas automatically because they share a
  `KAFKA_GROUP`.
- **Liveness/readiness probes** on the query API use `/healthz`.

These are teaching manifests: no resource tuning for real load, no TLS, no
network policies. Treat them as a starting point.
