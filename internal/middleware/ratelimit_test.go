package middleware

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/siddanagoudampatil/ai-api-gateway/internal/limiter"
)

func TestExtractClientIP(t *testing.T) {
	tests := []struct {
		name       string
		xff        string
		remoteAddr string
		expected   string
	}{
		{
			name:       "prefers leftmost ip in x-forwarded-for chain",
			xff:        "203.0.113.195, 70.41.3.18, 150.172.238.178",
			remoteAddr: "10.0.0.1:54321",
			expected:   "203.0.113.195",
		},
		{
			name:       "single ip in x-forwarded-for",
			xff:        "198.51.100.1",
			remoteAddr: "10.0.0.1:54321",
			expected:   "198.51.100.1",
		},
		{
			name:       "falls back to remoteaddr when xff is absent",
			xff:        "",
			remoteAddr: "192.168.1.50:61234",
			expected:   "192.168.1.50",
		},
		{
			name:       "parses ipv6 remoteaddr with port",
			xff:        "",
			remoteAddr: "[2001:db8::1]:8080",
			expected:   "2001:db8::1",
		},
		{
			name:       "handles bare remoteaddr without port",
			xff:        "",
			remoteAddr: "192.168.1.50",
			expected:   "192.168.1.50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.xff != "" {
				req.Header.Set("X-Forwarded-For", tt.xff)
			}
			req.RemoteAddr = tt.remoteAddr

			got := extractClientIP(req)
			if got != tt.expected {
				t.Errorf("extractClientIP() = %q, expected %q", got, tt.expected)
			}
		})
	}
}

func TestRateLimitMiddleware_Flow(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("skipping middleware integration test; redis not reachable: %v", err)
	}

	tokenLimiter, err := limiter.NewTokenBucketLimiter(limiter.Config{
		Client: rdb,
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("failed to initialize limiter: %v", err)
	}

	burst := int64(2)
	rate := 1.0 // 1 token per second
	mw := RateLimit(tokenLimiter, rate, burst, slog.New(slog.NewJSONHandler(io.Discard, nil)))

	nextCalled := 0
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	clientIP := "198.51.100.99"
	cleanupKey := "ratelimit:" + clientIP
	defer rdb.Del(context.Background(), cleanupKey)

	// Call 1: Allowed (burst 2 -> remaining 1)
	req1 := httptest.NewRequest(http.MethodGet, "/test", nil)
	req1.Header.Set("X-Forwarded-For", clientIP)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("call 1: expected status 200, got %d", rec1.Code)
	}
	if limit := rec1.Header().Get("X-RateLimit-Limit"); limit != "2" {
		t.Errorf("call 1: expected X-RateLimit-Limit '2', got %q", limit)
	}
	if remaining := rec1.Header().Get("X-RateLimit-Remaining"); remaining != "1" {
		t.Errorf("call 1: expected X-RateLimit-Remaining '1', got %q", remaining)
	}

	// Call 2: Allowed (burst 2 -> remaining 0)
	req2 := httptest.NewRequest(http.MethodGet, "/test", nil)
	req2.Header.Set("X-Forwarded-For", clientIP)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("call 2: expected status 200, got %d", rec2.Code)
	}
	if remaining := rec2.Header().Get("X-RateLimit-Remaining"); remaining != "0" {
		t.Errorf("call 2: expected X-RateLimit-Remaining '0', got %q", remaining)
	}

	// Call 3: Rejected (429 Too Many Requests)
	req3 := httptest.NewRequest(http.MethodGet, "/test", nil)
	req3.Header.Set("X-Forwarded-For", clientIP)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusTooManyRequests {
		t.Fatalf("call 3: expected status 429, got %d", rec3.Code)
	}
	if retryAfter := rec3.Header().Get("Retry-After"); retryAfter == "" {
		t.Error("call 3: expected non-empty Retry-After header")
	}
	if ct := rec3.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("call 3: expected Content-Type 'application/json', got %q", ct)
	}

	var body map[string]string
	if err := json.NewDecoder(rec3.Body).Decode(&body); err != nil {
		t.Fatalf("call 3: failed to parse JSON error response: %v", err)
	}
	if body["error"] != "Too Many Requests" || body["message"] != "rate limit exceeded" {
		t.Errorf("call 3: unexpected error payload: %+v", body)
	}

	// Next handler should only have been called twice (for the two allowed requests)
	if nextCalled != 2 {
		t.Errorf("expected downstream handler to be called 2 times, got %d", nextCalled)
	}
}

func TestRateLimitMiddleware_NilLimiterBypass(t *testing.T) {
	mw := RateLimit(nil, 10, 10, slog.New(slog.NewJSONHandler(io.Discard, nil)))

	called := false
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	handler.ServeHTTP(rec, req)

	if !called {
		t.Error("expected handler to be invoked when limiter is nil")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}
