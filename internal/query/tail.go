package query

// tail.go implements the live log tail over a WebSocket. This is the "tail -f"
// of the dashboard: the browser opens a socket with a filter, and the server
// streams matching logs as they land.
//
// Implementation choice — polling vs. streaming:
//
// We poll ClickHouse on a short interval, each time asking only for logs newer
// than the most recent timestamp we've already sent. This is simple, needs no
// extra infrastructure, and is correct as long as we advance a high-water mark.
// The trade-off is up to `pollInterval` of latency and some load on ClickHouse.
//
// The lower-latency alternative (documented, not built) is to attach a dedicated
// Kafka consumer to the `logs` topic and forward matching records straight to
// the socket — true real-time, at the cost of a second consumer path and more
// moving parts. For a learning platform, polling makes the mechanism obvious.
//
// WebSocket lifecycle care: we run a writer loop and a reader loop. The reader
// exists only to detect the client going away (a browser closing the tab); when
// it errors we cancel the context and the writer stops. This avoids the classic
// leak where the server keeps querying for a client that vanished.

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/1morshed1/gokraft/internal/storage/clickhouse"
)

var upgrader = websocket.Upgrader{
	// Allow any origin: this is a self-hosted internal tool. In a real
	// deployment you would restrict this to your dashboard's origin.
	CheckOrigin: func(*http.Request) bool { return true },
}

const (
	tailPollInterval = 1 * time.Second
	tailBatchLimit   = 200
)

func (h *Handlers) tailLogs(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return // Upgrade already wrote an error response
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()

	// Reader loop: we don't expect client messages, but reading is how gorilla
	// surfaces a closed connection. On any read error, cancel and bail.
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				cancel()
				return
			}
		}
	}()

	filter := clickhouse.SearchParams{
		Query:    c.Query("q"),
		Service:  c.Query("service"),
		Severity: c.Query("severity"),
		Limit:    tailBatchLimit,
	}

	// High-water mark: only send logs strictly newer than this. Start at "now"
	// so a tail shows new activity, not history.
	lastSeen := time.Now().UTC()

	ticker := time.NewTicker(tailPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q := filter
			q.From = lastSeen
			q.To = time.Now().UTC()
			logs, err := h.Logs.Search(ctx, q)
			if err != nil {
				// Transient: skip this tick, try again next.
				continue
			}
			// Search returns newest-first; send oldest-first so the client
			// appends in chronological order, and advance the high-water mark.
			for i := len(logs) - 1; i >= 0; i-- {
				l := logs[i]
				if !l.Timestamp.After(lastSeen) {
					continue // guard against boundary re-sends
				}
				if err := conn.WriteJSON(l); err != nil {
					return // client gone
				}
				lastSeen = l.Timestamp
			}
		}
	}
}
