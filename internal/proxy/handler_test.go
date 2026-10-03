package proxy

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestNewHandler_NilTarget(t *testing.T) {
	_, err := NewHandler(Config{})
	if err == nil {
		t.Fatal("expected error when TargetURL is nil, got nil")
	}
}

func TestReverseProxy_Success(t *testing.T) {
	backendReceivedReqID := ""
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendReceivedReqID = r.Header.Get("X-Request-ID")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer backend.Close()

	targetURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("failed to parse backend URL: %v", err)
	}

	handler, err := NewHandler(Config{
		TargetURL: targetURL,
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	if via := rec.Header().Get("Via"); via != "1.1 ai-api-gateway" {
		t.Errorf("expected Via header '1.1 ai-api-gateway', got %q", via)
	}

	if backendReceivedReqID == "" {
		t.Error("expected backend to receive X-Request-ID, got empty")
	}
}

func TestReverseProxy_BackendUnavailable(t *testing.T) {
	deadURL, err := url.Parse("http://127.0.0.1:54321")
	if err != nil {
		t.Fatalf("failed to parse dead URL: %v", err)
	}

	handler, err := NewHandler(Config{
		TargetURL: deadURL,
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected status 502, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("expected valid JSON body: %v", err)
	}

	if body["error"] != http.StatusText(http.StatusBadGateway) {
		t.Errorf("expected error message %q, got %v", http.StatusText(http.StatusBadGateway), body["error"])
	}
}

func TestReverseProxy_ContextTimeout(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	targetURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("failed to parse backend URL: %v", err)
	}

	handler, err := NewHandler(Config{
		TargetURL: targetURL,
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/slow", nil).WithContext(ctx)

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected status 504 on timeout, got %d", rec.Code)
	}
}
