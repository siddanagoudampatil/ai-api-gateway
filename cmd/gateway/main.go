package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/siddanagoudampatil/ai-api-gateway/internal/proxy"
)

type serverConfig struct {
	port         string
	backendURL   string
	logLevel     string
	readTimeout  time.Duration
	writeTimeout time.Duration
	idleTimeout  time.Duration
}

func loadConfig() serverConfig {
	port := os.Getenv("GATEWAY_PORT")
	if port == "" {
		port = os.Getenv("PORT")
	}
	if port == "" {
		port = "8080"
	}

	backendURL := os.Getenv("BACKEND_URL")
	if backendURL == "" {
		backendURL = os.Getenv("TARGET_URL")
	}
	if backendURL == "" {
		backendURL = "http://localhost:8081"
	}

	readTimeout := parseDuration(os.Getenv("READ_TIMEOUT"), 15*time.Second)
	writeTimeout := parseDuration(os.Getenv("WRITE_TIMEOUT"), 60*time.Second)
	idleTimeout := parseDuration(os.Getenv("IDLE_TIMEOUT"), 120*time.Second)

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	return serverConfig{
		port:         port,
		backendURL:   backendURL,
		logLevel:     logLevel,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		idleTimeout:  idleTimeout,
	}
}

func parseDuration(raw string, fallback time.Duration) time.Duration {
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return d
}

func setupLogger(levelStr string) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

func buildRouter(proxyHandler http.Handler) http.Handler {
	mux := http.NewServeMux()

	// Direct orchestrator liveness checks bypass reverse proxy hops to prevent
	// cascading liveness probe failures if downstream dependencies degrade.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "healthy",
			"time":   time.Now().UTC(),
		})
	})

	mux.Handle("/", proxyHandler)
	return mux
}

func main() {
	cfg := loadConfig()
	logger := setupLogger(cfg.logLevel)

	targetURL, err := url.Parse(cfg.backendURL)
	if err != nil || targetURL.Scheme == "" || targetURL.Host == "" {
		logger.Error("invalid upstream backend URL provided",
			slog.String("backend_url", cfg.backendURL),
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	proxyHandler, err := proxy.NewHandler(proxy.Config{
		TargetURL: targetURL,
		Logger:    logger,
	})
	if err != nil {
		logger.Error("failed to initialize reverse proxy engine", slog.Any("error", err))
		os.Exit(1)
	}

	router := buildRouter(proxyHandler)

	srv := &http.Server{
		Addr:         ":" + cfg.port,
		Handler:      router,
		ReadTimeout:  cfg.readTimeout,
		WriteTimeout: cfg.writeTimeout,
		IdleTimeout:  cfg.idleTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("starting api gateway",
			slog.String("addr", srv.Addr),
			slog.String("target_backend", targetURL.String()),
			slog.Duration("read_timeout", cfg.readTimeout),
			slog.Duration("write_timeout", cfg.writeTimeout),
		)

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server encountered fatal runtime error", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received; draining in-flight connections")

	// Allow pending requests to complete before closing the listener, avoiding aborted in-flight calls.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("forced shutdown due to context timeout", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("api gateway stopped cleanly")
}
