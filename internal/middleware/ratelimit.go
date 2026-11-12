package middleware

import (
	"encoding/json"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/siddanagoudampatil/ai-api-gateway/internal/limiter"
)

// RateLimit returns an HTTP middleware that throttles incoming traffic using a distributed
// Token Bucket algorithm backed by Redis.
//
// Key architectural trade-offs:
// 1. IP Extraction: Prioritizes the leftmost entry of X-Forwarded-For to identify the
//    originating client through proxy and CDN chains before falling back to RemoteAddr.
// 2. RFC Standards: Injects standard X-RateLimit-* telemetry headers and emits RFC 6585
//    compliant 429 Too Many Requests responses with a ceiling-rounded integer Retry-After header.
// 3. Fail-Open Resilience: Relies on the underlying limiter's fail-open design to maintain
//    gateway availability if Redis becomes partitioned.
func RateLimit(limiter *limiter.TokenBucketLimiter, rate float64, burst int64, logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		if limiter == nil {
			logger.Warn("rate limiter middleware initialized without limiter instance; bypassing checks")
			return next
		}

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			clientIP := extractClientIP(r)
			key := "ratelimit:" + clientIP

			res, _ := limiter.Allow(r.Context(), key, burst, rate)

			// Expose standard rate limit headers on all evaluated requests.
			w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(burst, 10))
			w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(res.Remaining, 10))

			if !res.Allowed {
				// RFC 7231 specifies Retry-After as an integer number of seconds.
				// We round up to the nearest whole second to guarantee that a retry
				// at that timestamp will find sufficient replenished capacity.
				retrySeconds := int64(math.Ceil(res.RetryAfter.Seconds()))
				if retrySeconds <= 0 {
					retrySeconds = 1
				}

				w.Header().Set("Retry-After", strconv.FormatInt(retrySeconds, 10))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error":   "Too Many Requests",
					"message": "rate limit exceeded",
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// extractClientIP resolves the originating client IP address.
//
// In multi-tier topologies (Load Balancers, API gateways, Cloudflare), the originating
// client IP is prepended as the leftmost token in X-Forwarded-For:
//
//	X-Forwarded-For: <client>, <proxy1>, <proxy2>
//
// If no proxy header exists, it strips the transport port from r.RemoteAddr.
func extractClientIP(r *http.Request) string {
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		if commaIdx := strings.Index(xff, ","); commaIdx != -1 {
			ip := strings.TrimSpace(xff[:commaIdx])
			if ip != "" {
				return ip
			}
		} else {
			ip := strings.TrimSpace(xff)
			if ip != "" {
				return ip
			}
		}
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return strings.TrimSpace(r.RemoteAddr)
	}
	return host
}
