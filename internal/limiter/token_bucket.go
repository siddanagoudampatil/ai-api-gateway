package limiter

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed token_bucket.lua
var tokenBucketLua string

// Result encapsulates the outcome of a rate limiting evaluation.
type Result struct {
	// Allowed indicates whether the request passed rate limiting.
	Allowed bool
	// Remaining is the integer count of tokens remaining in the bucket.
	Remaining int64
	// RetryAfter is the wait duration before enough tokens replenish to satisfy the request.
	RetryAfter time.Duration
}

// Config defines dependencies for TokenBucketLimiter.
type Config struct {
	Client redis.UniversalClient
	Logger *slog.Logger
}

// TokenBucketLimiter coordinates distributed token-bucket rate limiting backed by Redis.
// Evaluations execute atomically via an embedded Lua script to prevent race conditions
// across horizontally scaled gateway instances.
type TokenBucketLimiter struct {
	client redis.UniversalClient
	script *redis.Script
	logger *slog.Logger
}

// NewTokenBucketLimiter initializes a rate limiter with the embedded Lua script pre-compiled.
func NewTokenBucketLimiter(cfg Config) (*TokenBucketLimiter, error) {
	if cfg.Client == nil {
		return nil, errors.New("redis client must not be nil")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &TokenBucketLimiter{
		client: cfg.Client,
		// redis.NewScript caches SHA1 client-side, executing EVALSHA first
		// and transparently falling back to EVAL on NOSCRIPT.
		script: redis.NewScript(tokenBucketLua),
		logger: logger,
	}, nil
}

// Allow evaluates whether 1 token is available for the given key.
func (l *TokenBucketLimiter) Allow(ctx context.Context, key string, burst int64, rate float64) (Result, error) {
	return l.AllowN(ctx, key, burst, rate, 1)
}

// AllowN evaluates whether n tokens are available for the given key.
//
// Fail-Open Resilience Strategy:
// If Redis becomes unreachable, commands timeout, or script execution fails,
// AllowN logs a structured warning and returns Allowed: true alongside the error.
// In API gateway routing, preserving downstream availability during transient caching
// infrastructure partitions is prioritized over strict abuse mitigation.
func (l *TokenBucketLimiter) AllowN(ctx context.Context, key string, burst int64, rate float64, tokens int64) (Result, error) {
	if burst <= 0 || rate <= 0 {
		return Result{Allowed: true, Remaining: burst, RetryAfter: 0}, errors.New("burst and rate must be greater than zero")
	}
	if tokens <= 0 {
		tokens = 1
	}

	nowMs := time.Now().UnixMilli()

	raw, err := l.script.Run(ctx, l.client, []string{key}, burst, rate, nowMs, tokens).Result()
	if err != nil {
		l.logger.WarnContext(ctx, "rate limiter degraded: redis execution failed, failing open",
			slog.String("key", key),
			slog.String("error", err.Error()),
		)
		return Result{
			Allowed:    true,
			Remaining:  burst,
			RetryAfter: 0,
		}, err
	}

	resSlice, ok := raw.([]any)
	if !ok {
		if ifaceSlice, okIface := raw.([]interface{}); okIface {
			resSlice = ifaceSlice
		} else {
			err = fmt.Errorf("unexpected script response type: %T", raw)
			return l.failOpen(ctx, key, burst, err)
		}
	}

	if len(resSlice) < 3 {
		err = fmt.Errorf("insufficient return values from lua script: got %d, expected 3", len(resSlice))
		return l.failOpen(ctx, key, burst, err)
	}

	allowedVal, err := parseNumber(resSlice[0])
	if err != nil {
		return l.failOpen(ctx, key, burst, err)
	}

	remainingVal, err := parseNumber(resSlice[1])
	if err != nil {
		return l.failOpen(ctx, key, burst, err)
	}

	retryAfterMsVal, err := parseNumber(resSlice[2])
	if err != nil {
		return l.failOpen(ctx, key, burst, err)
	}

	var retryAfter time.Duration
	if retryAfterMsVal > 0 {
		retryAfter = time.Duration(retryAfterMsVal) * time.Millisecond
	}

	return Result{
		Allowed:    allowedVal == 1,
		Remaining:  remainingVal,
		RetryAfter: retryAfter,
	}, nil
}

func (l *TokenBucketLimiter) failOpen(ctx context.Context, key string, burst int64, err error) (Result, error) {
	l.logger.WarnContext(ctx, "rate limiter degraded: parse failure, failing open",
		slog.String("key", key),
		slog.String("error", err.Error()),
	)
	return Result{
		Allowed:    true,
		Remaining:  burst,
		RetryAfter: 0,
	}, err
}

func parseNumber(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case float64:
		return int64(n), nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	default:
		return 0, fmt.Errorf("cannot parse %T as int64", v)
	}
}
