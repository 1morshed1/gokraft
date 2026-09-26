package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// LogNotifier just logs fire/resolve events. It is the default when a rule has
// no NotifyURL, and is handy in development.
type LogNotifier struct{ Log *slog.Logger }

func (n LogNotifier) Fire(_ context.Context, ev Event) {
	n.Log.Warn("ALERT FIRING", "rule", ev.Rule.Name, "severity", ev.Rule.Severity, "value", ev.Value, "threshold", ev.Rule.Threshold)
}
func (n LogNotifier) Resolve(_ context.Context, ev Event) {
	n.Log.Info("ALERT RESOLVED", "rule", ev.Rule.Name, "value", ev.Value)
}

// WebhookNotifier POSTs a JSON payload to a rule's NotifyURL (a Slack incoming
// webhook, an email gateway, PagerDuty, etc.). It falls back to a LogNotifier
// for rules without a URL so nothing is silently dropped.
//
// Real-world care baked in: a short client timeout so a slow/hung webhook can't
// stall the evaluation loop, and best-effort delivery (a failed POST is logged,
// not retried forever — an alert you can't deliver shouldn't wedge the engine).
type WebhookNotifier struct {
	Client   *http.Client
	Log      *slog.Logger
	fallback LogNotifier
}

// NewWebhookNotifier builds a notifier with a sane timeout.
func NewWebhookNotifier(log *slog.Logger) *WebhookNotifier {
	return &WebhookNotifier{
		Client:   &http.Client{Timeout: 5 * time.Second},
		Log:      log,
		fallback: LogNotifier{Log: log},
	}
}

func (n *WebhookNotifier) Fire(ctx context.Context, ev Event)    { n.post(ctx, ev, "firing") }
func (n *WebhookNotifier) Resolve(ctx context.Context, ev Event) { n.post(ctx, ev, "resolved") }

func (n *WebhookNotifier) post(ctx context.Context, ev Event, status string) {
	if ev.Rule.NotifyURL == "" {
		if status == "firing" {
			n.fallback.Fire(ctx, ev)
		} else {
			n.fallback.Resolve(ctx, ev)
		}
		return
	}
	payload := map[string]any{
		"status":    status,
		"rule":      ev.Rule.Name,
		"severity":  ev.Rule.Severity,
		"metric":    ev.Rule.Metric,
		"service":   ev.Rule.Service,
		"value":     ev.Value,
		"threshold": ev.Rule.Threshold,
		"timestamp": ev.Timestamp.Format(time.RFC3339),
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ev.Rule.NotifyURL, bytes.NewReader(body))
	if err != nil {
		n.Log.Error("build webhook request", "rule", ev.Rule.ID, "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.Client.Do(req)
	if err != nil {
		n.Log.Error("webhook delivery failed", "rule", ev.Rule.ID, "err", err)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		n.Log.Error("webhook non-2xx", "rule", ev.Rule.ID, "status", resp.StatusCode)
	}
}
