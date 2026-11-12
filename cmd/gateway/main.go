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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/siddanagoudampatil/ai-api-gateway/internal/limiter"
	"github.com/siddanagoudampatil/ai-api-gateway/internal/middleware"
	"github.com/siddanagoudampatil/ai-api-gateway/internal/proxy"
)

type serverConfig struct {
	port           string
	backendURL     string
	redisAddr      string
	rateLimitRPS   float64
	rateLimitBurst int64
	logLevel       string
	readTimeout    time.Duration
	writeTimeout   time.Duration
	idleTimeout    time.Duration
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

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rateLimitRPS := parseFloat(os.Getenv("RATE_LIMIT_RPS"), 5.0)
	rateLimitBurst := parseInt64(os.Getenv("RATE_LIMIT_BURST"), 10)

	readTimeout := parseDuration(os.Getenv("READ_TIMEOUT"), 15*time.Second)
	writeTimeout := parseDuration(os.Getenv("WRITE_TIMEOUT"), 60*time.Second)
	idleTimeout := parseDuration(os.Getenv("IDLE_TIMEOUT"), 120*time.Second)

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	return serverConfig{
		port:           port,
		backendURL:     backendURL,
		redisAddr:      redisAddr,
		rateLimitRPS:   rateLimitRPS,
		rateLimitBurst: rateLimitBurst,
		logLevel:       logLevel,
		readTimeout:    readTimeout,
		writeTimeout:   writeTimeout,
		idleTimeout:    idleTimeout,
	}
}

func parseFloat(raw string, fallback float64) float64 {
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func parseInt64(raw string, fallback int64) int64 {
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
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

func buildRouter(proxyHandler http.Handler, rateLimiter *limiter.TokenBucketLimiter, rps float64, burst int64, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	// Direct orchestrator liveness checks bypass reverse proxy hops and rate limiting
	// to prevent cascading liveness probe failures if downstream dependencies degrade.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "healthy",
			"time":   time.Now().UTC(),
		})
	})

	rateLimitedProxy := middleware.RateLimit(rateLimiter, rps, burst, logger)(proxyHandler)
	mux.Handle("/", rateLimitedProxy)
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

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.redisAddr,
		PoolSize:     100,
		MinIdleConns: 10,
		DialTimeout:  5 * time.Second,
	})

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		logger.Warn("redis connection check failed at boot; rate limiting will operate in fail-open mode",
			slog.String("redis_addr", cfg.redisAddr),
			slog.Any("error", err),
		)
	} else {
		logger.Info("connected to redis", slog.String("redis_addr", cfg.redisAddr))
	}
	pingCancel()

	tokenLimiter, err := limiter.NewTokenBucketLimiter(limiter.Config{
		Client: rdb,
		Logger: logger,
	})
	if err != nil {
		logger.Error("failed to initialize token bucket limiter", slog.Any("error", err))
		os.Exit(1)
	}

	router := buildRouter(proxyHandler, tokenLimiter, cfg.rateLimitRPS, cfg.rateLimitBurst, logger)

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
			slog.String("redis_addr", cfg.redisAddr),
			slog.Float64("rate_limit_rps", cfg.rateLimitRPS),
			slog.Int64("rate_limit_burst", cfg.rateLimitBurst),
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
	}

	if err := rdb.Close(); err != nil {
		logger.Error("failed to close redis connection pool cleanly", slog.Any("error", err))
	} else {
		logger.Info("redis connection pool closed cleanly")
	}

	logger.Info("api gateway stopped cleanly")
}
