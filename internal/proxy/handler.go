package proxy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

// Config defines dependencies and tuning parameters for the reverse proxy.
type Config struct {
	TargetURL     *url.URL
	Logger        *slog.Logger
	Transport     http.RoundTripper
	FlushInterval time.Duration
}

// Handler handles HTTP traffic forwarding to upstream services.
type Handler struct {
	target *url.URL
	proxy  *httputil.ReverseProxy
	logger *slog.Logger
}

// NewHandler initializes a reverse proxy instance with tuned connection pooling,
// streaming support for real-time model outputs, and structured failure logging.
func NewHandler(cfg Config) (*Handler, error) {
	if cfg.TargetURL == nil {
		return nil, errors.New("target URL must not be nil")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	transport := cfg.Transport
	if transport == nil {
		// net.http.DefaultTransport caps MaxIdleConnsPerHost at 2. For gateway architectures
		// routing high-throughput traffic to a bounded set of backends, this default causes
		// rapid TCP connection churn and ephemeral port exhaustion.
		transport = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          1000,
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
		}
	}

	// Use negative interval to immediately flush chunks as received. This is vital
	// for AI workloads (e.g. SSE token streams) to prevent buffering delays.
	flushInterval := cfg.FlushInterval
	if flushInterval == 0 {
		flushInterval = -1 * time.Millisecond
	}

	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			// Rewrite replaces the legacy Director callback, safely sanitizing hop-by-hop headers
			// and standardizing X-Forwarded-* headers without leaking internal network topologies.
			pr.SetURL(cfg.TargetURL)
			pr.SetXForwarded()

			if pr.Out.Header.Get("X-Request-ID") == "" {
				pr.Out.Header.Set("X-Request-ID", generateRequestID())
			}
		},
		Transport:     transport,
		FlushInterval: flushInterval,
		ModifyResponse: func(resp *http.Response) error {
			resp.Header.Set("Via", "1.1 ai-api-gateway")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			status := http.StatusBadGateway
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				status = http.StatusGatewayTimeout
			}

			logger.ErrorContext(r.Context(), "upstream proxy dispatch failed",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.String("target", cfg.TargetURL.String()),
				slog.Int("status", status),
				slog.String("error", err.Error()),
			)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error":   http.StatusText(status),
				"message": "unable to route request to upstream service",
			})
		},
	}

	return &Handler{
		target: cfg.TargetURL,
		proxy:  rp,
		logger: logger,
	}, nil
}

// ServeHTTP delegates request handling to the configured reverse proxy engine.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.proxy.ServeHTTP(w, r)
}

// Target returns the configured upstream backend URL.
func (h *Handler) Target() *url.URL {
	return h.target
}

func generateRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}
