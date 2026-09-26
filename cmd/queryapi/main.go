// Command queryapi is the read side of gokraft. It exposes the REST + WebSocket
// query API over the three stores, runs the alerting engine in the background,
// and serves the static dashboard.
//
// It is the one process that talks to all three databases (read-only), because
// cross-signal correlation inherently spans them. The writers stay single-store;
// only the query side needs the whole picture.
//
//	HTTP_ADDR       default ":8080"
//	CLICKHOUSE_ADDR default "localhost:9000"   (+ _DB/_USER/_PASS)
//	TIMESCALE_DSN   default "postgres://gokraft:gokraft@localhost:5433/metrics"
//	POSTGRES_DSN    default "postgres://gokraft:gokraft@localhost:5434/traces"
//	ALERT_INTERVAL  default "30s"
//	WEB_DIR         default "web"
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/1morshed1/gokraft/internal/alert"
	"github.com/1morshed1/gokraft/internal/config"
	"github.com/1morshed1/gokraft/internal/logging"
	"github.com/1morshed1/gokraft/internal/query"
	"github.com/1morshed1/gokraft/internal/storage/clickhouse"
	"github.com/1morshed1/gokraft/internal/storage/timescale"
	"github.com/1morshed1/gokraft/internal/storage/tracestore"
)

func main() {
	log := logging.New("queryapi")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- open the three stores ---
	logStore, err := clickhouse.Open(
		config.String("CLICKHOUSE_ADDR", "localhost:9000"),
		config.String("CLICKHOUSE_DB", "gokraft"),
		config.String("CLICKHOUSE_USER", "default"),
		config.String("CLICKHOUSE_PASS", ""),
	)
	if err != nil {
		log.Error("connect clickhouse", "err", err)
		os.Exit(1)
	}
	defer logStore.Close()

	metricStore, err := timescale.Open(ctx, config.String("TIMESCALE_DSN", "postgres://gokraft:gokraft@localhost:5433/metrics"))
	if err != nil {
		log.Error("connect timescale", "err", err)
		os.Exit(1)
	}
	defer metricStore.Close()

	traceStore, err := tracestore.Open(ctx, config.String("POSTGRES_DSN", "postgres://gokraft:gokraft@localhost:5434/traces"))
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer traceStore.Close()

	// --- alerting engine ---
	rules := alert.NewRuleStore()
	seedDefaultRules(rules, log)
	notifier := alert.NewWebhookNotifier(log)
	evaluator := alert.NewEvaluator(rules, metricStore.Scalar, notifier, log)
	go evaluator.Run(ctx, config.Duration("ALERT_INTERVAL", 30*time.Second))

	// --- HTTP server ---
	if os.Getenv("GIN_MODE") == "" {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.Use(gin.Recovery(), requestLogger(log))

	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

	api := r.Group("/api/v1")
	(&query.Handlers{Logs: logStore, Metrics: metricStore, Traces: traceStore}).Register(api)
	(&alert.API{Rules: rules, Eval: evaluator}).Register(api)

	// Static dashboard.
	webDir := config.String("WEB_DIR", "web")
	r.StaticFile("/", webDir+"/index.html")
	r.Static("/static", webDir)

	srv := &http.Server{Addr: config.String("HTTP_ADDR", ":8080"), Handler: r}
	go func() {
		log.Info("query API listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down query API")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

// requestLogger is a tiny structured access log middleware. We write our own
// (instead of gin.Logger) so it emits JSON consistent with the rest of gokraft.
func requestLogger(log interface {
	Info(string, ...any)
}) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info("http",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"dur_ms", time.Since(start).Milliseconds(),
		)
	}
}

// seedDefaultRules installs a couple of demonstrative rules so the dashboard has
// something to show on a fresh start. They match the metrics the load generator
// emits (see cmd/loadgen).
func seedDefaultRules(rs *alert.RuleStore, log interface {
	Error(string, ...any)
}) {
	defaults := []alert.Rule{
		{
			Name: "High error rate (api)", Metric: "http_error_rate", Service: "api",
			Aggregation: "avg", Comparator: alert.GT, Threshold: 0.05,
			Window: 5 * time.Minute, For: 2 * time.Minute, Severity: alert.Critical,
		},
		{
			Name: "High request latency (checkout)", Metric: "http_request_duration_ms", Service: "checkout",
			Aggregation: "avg", Comparator: alert.GT, Threshold: 500,
			Window: 5 * time.Minute, For: 1 * time.Minute, Severity: alert.Warning,
		},
	}
	for _, r := range defaults {
		if _, err := rs.Upsert(r); err != nil {
			log.Error("seed rule", "err", err)
		}
	}
}
