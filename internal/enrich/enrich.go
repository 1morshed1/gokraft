// Package enrich attaches Kubernetes context to every telemetry signal before
// it is forwarded to Kafka.
//
// Why enrich at all? An app emits "service=checkout, error=timeout". That tells
// you *what* happened but not *where*. In a cluster running dozens of replicas
// across many nodes, "which pod on which node in which namespace produced this?"
// is the difference between a five-minute and a five-hour incident. The
// collector is the right place to answer it because it sits at the ingress and
// can see the sender's identity, whereas the app itself often does not know its
// own pod name.
//
// Two ways to resolve pod identity:
//
//  1. The Downward API (what we implement). Kubernetes injects the pod's own
//     name/namespace/node into the container as environment variables via the
//     pod spec. This is simple, requires no API-server permissions, and is
//     appropriate when the collector runs as a DaemonSet/sidecar and enriches
//     with *its own* pod context, or when apps forward their identity.
//
//  2. Querying the API server by source IP (documented, not wired in). The
//     collector takes the gRPC peer IP, asks the Kubernetes API "which pod has
//     status.podIP == X?", and caches the answer for ~60s. This resolves the
//     *sender's* identity even when the sender didn't self-report it, but it
//     needs RBAC to list pods and pulls in the heavy client-go dependency. See
//     docs/learning/01-collector.md for the design; the Enricher interface
//     below is intentionally shaped so an APIServerEnricher can be dropped in
//     later without touching callers.
//
// Keeping enrichment behind an interface is the lesson here: the collector
// depends on the *capability* ("give me K8s metadata for this sender"), not on
// any particular implementation. That is dependency inversion, and it keeps the
// hot path testable with a trivial fake.
package enrich

import (
	"github.com/1morshed1/gokraft/internal/model"
)

// Enricher resolves Kubernetes metadata for a signal given the sender's source
// address (host:port or IP). Implementations must be safe for concurrent use —
// the collector calls Enrich from many gRPC handler goroutines at once.
type Enricher interface {
	// Metadata returns the K8s metadata to attach for a signal from sourceAddr.
	// Implementations should never block for long on the hot path; cache
	// aggressively.
	Metadata(sourceAddr string) model.K8sMetadata
}

// StaticEnricher returns a fixed K8sMetadata for every signal, populated from
// the Downward API environment variables at startup. In dev/local mode where
// those vars are unset it returns an empty struct, which is harmless — the
// storage columns simply stay blank.
//
// It carries no per-request state and needs no locking, so it is trivially
// concurrency-safe.
type StaticEnricher struct {
	meta model.K8sMetadata
}

// NewStaticFromEnv builds a StaticEnricher from the standard Downward API vars.
// A typical pod spec sets these with fieldRef references:
//
//	env:
//	  - name: K8S_POD_NAME
//	    valueFrom: { fieldRef: { fieldPath: metadata.name } }
//	  - name: K8S_NAMESPACE
//	    valueFrom: { fieldRef: { fieldPath: metadata.namespace } }
//	  - name: K8S_NODE_NAME
//	    valueFrom: { fieldRef: { fieldPath: spec.nodeName } }
func NewStaticFromEnv(getenv func(string) string) *StaticEnricher {
	return &StaticEnricher{
		meta: model.K8sMetadata{
			PodName:   getenv("K8S_POD_NAME"),
			Namespace: getenv("K8S_NAMESPACE"),
			NodeName:  getenv("K8S_NODE_NAME"),
		},
	}
}

// Metadata ignores sourceAddr and returns the fixed metadata.
func (s *StaticEnricher) Metadata(string) model.K8sMetadata {
	return s.meta
}
