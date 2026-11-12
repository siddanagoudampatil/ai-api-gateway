package limiter

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestNewTokenBucketLimiter_NilClient(t *testing.T) {
	_, err := NewTokenBucketLimiter(Config{
		Client: nil,
	})
	if err == nil {
		t.Fatal("expected error when redis client is nil, got nil")
	}
}

func TestTokenBucketLimiter_Validation(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	limiter, err := NewTokenBucketLimiter(Config{
		Client: rdb,
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	ctx := context.Background()

	// Invalid burst
	res, err := limiter.Allow(ctx, "test:invalid_burst", 0, 10.0)
	if err == nil || !res.Allowed {
		t.Errorf("expected error and fail-open Allowed=true for burst=0, got res=%+v, err=%v", res, err)
	}

	// Invalid rate
	res, err = limiter.Allow(ctx, "test:invalid_rate", 10, 0.0)
	if err == nil || !res.Allowed {
		t.Errorf("expected error and fail-open Allowed=true for rate=0, got res=%+v, err=%v", res, err)
	}
}

func TestTokenBucketLimiter_FailOpenOnRedisFailure(t *testing.T) {
	// Point to an invalid, unreachable address to verify resilience behavior
	unreachableClient := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:54321",
		DialTimeout: 50 * time.Millisecond,
		MaxRetries:  0,
	})
	defer unreachableClient.Close()

	limiter, err := NewTokenBucketLimiter(Config{
		Client: unreachableClient,
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("unexpected initialization error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	res, err := limiter.Allow(ctx, "test:fail_open_client", 5, 2.0)
	// Fail-open contract: must allow traffic despite underlying Redis failure
	if !res.Allowed {
		t.Errorf("expected Allowed=true during Redis partition, got false")
	}
	if res.Remaining != 5 {
		t.Errorf("expected Remaining=5 during Redis partition, got %d", res.Remaining)
	}
	if err == nil {
		t.Error("expected non-nil error indicating Redis failure, got nil")
	}
}

func setupTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		t.Skipf("skipping redis test; redis not reachable on localhost:6379: %v", err)
	}

	t.Cleanup(func() {
		_ = rdb.Close()
	})

	return rdb
}

func TestTokenBucketLimiter_BurstAllowance(t *testing.T) {
	rdb := setupTestRedis(t)

	limiter, err := NewTokenBucketLimiter(Config{
		Client: rdb,
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	testKey := "ratelimit:test:burst:" + time.Now().Format("20060102150405.000000000")
	defer rdb.Del(context.Background(), testKey)

	ctx := context.Background()
	burst := int64(5)
	rate := 1.0 // 1 token per second

	// Exhaust exactly the burst allowance
	for i := int64(0); i < burst; i++ {
		res, err := limiter.Allow(ctx, testKey, burst, rate)
		if err != nil {
			t.Fatalf("call %d returned unexpected error: %v", i+1, err)
		}
		if !res.Allowed {
			t.Fatalf("call %d: expected Allowed=true within burst, got false", i+1)
		}
		expectedRemaining := burst - 1 - i
		if res.Remaining != expectedRemaining {
			t.Errorf("call %d: expected Remaining=%d, got %d", i+1, expectedRemaining, res.Remaining)
		}
		if res.RetryAfter != 0 {
			t.Errorf("call %d: expected RetryAfter=0 for allowed request, got %v", i+1, res.RetryAfter)
		}
	}
}

func TestTokenBucketLimiter_SubsequentRejection(t *testing.T) {
	rdb := setupTestRedis(t)

	limiter, err := NewTokenBucketLimiter(Config{
		Client: rdb,
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	testKey := "ratelimit:test:rejection:" + time.Now().Format("20060102150405.000000000")
	defer rdb.Del(context.Background(), testKey)

	ctx := context.Background()
	burst := int64(2)
	rate := 1.0

	// Consume entire burst capacity
	for i := 0; i < int(burst); i++ {
		res, err := limiter.Allow(ctx, testKey, burst, rate)
		if err != nil || !res.Allowed {
			t.Fatalf("preconditioning call %d failed: res=%+v, err=%v", i+1, res, err)
		}
	}

	// Subsequent request must be rejected
	rejectedRes, err := limiter.Allow(ctx, testKey, burst, rate)
	if err != nil {
		t.Fatalf("unexpected error on rejected call: %v", err)
	}
	if rejectedRes.Allowed {
		t.Fatalf("expected Allowed=false after burst exhaustion, got true")
	}
	if rejectedRes.Remaining != 0 {
		t.Errorf("expected Remaining=0 on rejection, got %d", rejectedRes.Remaining)
	}
	if rejectedRes.RetryAfter <= 0 {
		t.Errorf("expected positive RetryAfter duration on rejection, got %v", rejectedRes.RetryAfter)
	}

	// Immediate next request must also be rejected
	immediateRes, err := limiter.Allow(ctx, testKey, burst, rate)
	if err != nil {
		t.Fatalf("unexpected error on repeated rejected call: %v", err)
	}
	if immediateRes.Allowed {
		t.Fatalf("expected Allowed=false on repeated request, got true")
	}
}

func TestTokenBucketLimiter_TokenReplenishment(t *testing.T) {
	rdb := setupTestRedis(t)

	limiter, err := NewTokenBucketLimiter(Config{
		Client: rdb,
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	testKey := "ratelimit:test:replenish:" + time.Now().Format("20060102150405.000000000")
	defer rdb.Del(context.Background(), testKey)

	ctx := context.Background()
	burst := int64(2)
	rate := 5.0 // 5 tokens per second = 1 token per 200ms

	// Drain bucket
	for i := 0; i < int(burst); i++ {
		res, err := limiter.Allow(ctx, testKey, burst, rate)
		if err != nil || !res.Allowed {
			t.Fatalf("pre-drain call %d failed: res=%+v, err=%v", i+1, res, err)
		}
	}

	// Verify drained
	exhaustedRes, err := limiter.Allow(ctx, testKey, burst, rate)
	if err != nil || exhaustedRes.Allowed {
		t.Fatalf("expected bucket to be exhausted, got res=%+v, err=%v", exhaustedRes, err)
	}

	// Wait 250ms for replenishment (>= 1.25 tokens at 5 tokens/sec)
	time.Sleep(250 * time.Millisecond)

	// Post-replenishment call must succeed
	replenishedRes, err := limiter.Allow(ctx, testKey, burst, rate)
	if err != nil {
		t.Fatalf("unexpected error after replenishment window: %v", err)
	}
	if !replenishedRes.Allowed {
		t.Fatalf("expected Allowed=true after token replenishment, got false (RetryAfter=%v)", replenishedRes.RetryAfter)
	}
}

func TestTokenBucketLimiter_AllowN(t *testing.T) {
	rdb := setupTestRedis(t)

	limiter, err := NewTokenBucketLimiter(Config{
		Client: rdb,
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	testKey := "ratelimit:test:allown:" + time.Now().Format("20060102150405.000000000")
	defer rdb.Del(context.Background(), testKey)

	ctx := context.Background()
	burst := int64(10)
	rate := 5.0

	// Consume 6 tokens at once: Allowed (10 -> 4)
	res1, err := limiter.AllowN(ctx, testKey, burst, rate, 6)
	if err != nil || !res1.Allowed || res1.Remaining != 4 {
		t.Fatalf("AllowN(6): expected Allowed=true, Remaining=4, got res=%+v, err=%v", res1, err)
	}

	// Request 5 tokens: Rejected (only 4 tokens remaining)
	res2, err := limiter.AllowN(ctx, testKey, burst, rate, 5)
	if err != nil || res2.Allowed {
		t.Fatalf("AllowN(5): expected Allowed=false, got res=%+v, err=%v", res2, err)
	}
	if res2.RetryAfter <= 0 {
		t.Errorf("AllowN(5): expected positive RetryAfter, got %v", res2.RetryAfter)
	}

	// Request 4 tokens: Allowed (4 -> 0)
	res3, err := limiter.AllowN(ctx, testKey, burst, rate, 4)
	if err != nil || !res3.Allowed || res3.Remaining != 0 {
		t.Fatalf("AllowN(4): expected Allowed=true, Remaining=0, got res=%+v, err=%v", res3, err)
	}
}

func TestTokenBucketLimiter_RedisIntegration(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("skipping redis integration tests; redis not reachable on localhost:6379: %v", err)
	}

	limiter, err := NewTokenBucketLimiter(Config{
		Client: rdb,
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	testKey := "ratelimit:test:integration:" + time.Now().Format("20060102150405.000000")
	defer rdb.Del(context.Background(), testKey)

	burst := int64(3)
	rate := 2.0 // 2 tokens per second

	// 1. Initial request from fresh client consumes 1 token from burst
	res1, err := limiter.Allow(ctx, testKey, burst, rate)
	if err != nil {
		t.Fatalf("unexpected error on call 1: %v", err)
	}
	if !res1.Allowed || res1.Remaining != 2 {
		t.Errorf("call 1: expected Allowed=true, Remaining=2, got Allowed=%v, Remaining=%d", res1.Allowed, res1.Remaining)
	}

	// 2. Consume remaining burst
	res2, err := limiter.Allow(ctx, testKey, burst, rate)
	if err != nil || !res2.Allowed || res2.Remaining != 1 {
		t.Errorf("call 2: expected Allowed=true, Remaining=1, got Allowed=%v, Remaining=%d, err=%v", res2.Allowed, res2.Remaining, err)
	}

	res3, err := limiter.Allow(ctx, testKey, burst, rate)
	if err != nil || !res3.Allowed || res3.Remaining != 0 {
		t.Errorf("call 3: expected Allowed=true, Remaining=0, got Allowed=%v, Remaining=%d, err=%v", res3.Allowed, res3.Remaining, err)
	}

	// 3. Bucket exhausted -> must be throttled
	res4, err := limiter.Allow(ctx, testKey, burst, rate)
	if err != nil {
		t.Fatalf("unexpected error on throttled call: %v", err)
	}
	if res4.Allowed {
		t.Errorf("call 4: expected Allowed=false on empty bucket, got true")
	}
	if res4.RetryAfter <= 0 {
		t.Errorf("call 4: expected positive RetryAfter, got %v", res4.RetryAfter)
	}

	// 4. Verify Redis key TTL is configured to avoid memory leaks
	ttl := rdb.TTL(ctx, testKey).Val()
	if ttl <= 0 {
		t.Errorf("expected positive TTL on rate limit key, got %v", ttl)
	}

	// 5. Wait for token replenishment (600ms at 2 tokens/sec adds ~1.2 tokens)
	time.Sleep(600 * time.Millisecond)

	res5, err := limiter.Allow(ctx, testKey, burst, rate)
	if err != nil {
		t.Fatalf("unexpected error after replenishment: %v", err)
	}
	if !res5.Allowed {
		t.Errorf("call 5: expected Allowed=true after refill window, got false (RetryAfter=%v)", res5.RetryAfter)
	}
}
