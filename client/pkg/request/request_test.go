package request

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/client/conf"
	"github.com/calypr/syfon/client/pkg/logs"
)

func TestNewRequestInterface(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cred := &conf.Credential{
		KeyID:       "test-key",
		APIKey:      "test-secret",
		APIEndpoint: "https://example.com",
	}

	// Create a mock config manager
	mockConf := &mockConfigManager{}

	reqInterface := NewRequestInterface(logs.NewGen3Logger(logger, "", ""), cred, mockConf, "https://example.com", "test-ua", nil)

	if reqInterface == nil {
		t.Fatal("Expected non-nil request interface")
	}

	req, ok := reqInterface.(*Request)
	if !ok {
		t.Fatal("Expected request interface to be of type *Request")
	}

	if req.BaseURL != "https://example.com" {
		t.Errorf("Expected BaseURL 'https://example.com', got '%s'", req.BaseURL)
	}

	if req.UserAgent != "test-ua" {
		t.Errorf("Expected UserAgent 'test-ua', got '%s'", req.UserAgent)
	}
}

func TestRequestBuilder_New(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	mockConf := &mockConfigManager{}

	reqInterface := NewRequestInterface(logs.NewGen3Logger(logger, "", ""), nil, mockConf, "https://example.com", "test-ua", nil)
	req := reqInterface.(*Request)

	// Test relative path
	builder := req.New("GET", "/api/test")
	if builder.Url != "https://example.com/api/test" {
		t.Errorf("Expected URL 'https://example.com/api/test', got '%s'", builder.Url)
	}

	// Test absolute URL
	builder = req.New("GET", "https://other.com/api/test")
	if builder.Url != "https://other.com/api/test" {
		t.Errorf("Expected URL 'https://other.com/api/test', got '%s'", builder.Url)
	}
}

func TestRequestBuilder_WithHeaders(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	mockConf := &mockConfigManager{}

	reqInterface := NewRequestInterface(logs.NewGen3Logger(logger, "", ""), nil, mockConf, "https://example.com", "test-ua", nil)
	req := reqInterface.(*Request)

	builder := req.New("GET", "/api/test")
	builder = builder.WithHeader("X-Custom-Header", "test-value")

	if builder.Headers["X-Custom-Header"] != "test-value" {
		t.Error("Expected X-Custom-Header to be set")
	}
}

func TestRequestBuilder_WithToken(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	mockConf := &mockConfigManager{}

	reqInterface := NewRequestInterface(logs.NewGen3Logger(logger, "", ""), nil, mockConf, "https://example.com", "test-ua", nil)
	req := reqInterface.(*Request)

	token := "test-bearer-token-12345"
	builder := req.New("GET", "/api/test")
	builder = builder.WithToken(token)

	if builder.Token != token {
		t.Errorf("Expected token '%s', got '%s'", token, builder.Token)
	}
}

func TestRequestBuilder_WithBody(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	mockConf := &mockConfigManager{}

	reqInterface := NewRequestInterface(logs.NewGen3Logger(logger, "", ""), nil, mockConf, "https://example.com", "test-ua", nil)
	req := reqInterface.(*Request)

	body := strings.NewReader("test body content")
	builder := req.New("POST", "/api/test")
	builder = builder.WithBody(body)

	if builder.Body == nil {
		t.Error("Expected non-nil body")
	}
}

func TestRequest_Do_Success(t *testing.T) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the request
		if r.Method != "GET" {
			t.Errorf("Expected GET method, got %s", r.Method)
		}
		if r.Header.Get("User-Agent") != "test-ua" {
			t.Errorf("Expected User-Agent 'test-ua', got '%s'", r.Header.Get("User-Agent"))
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("Expected Authorization 'Bearer test-token', got '%s'", r.Header.Get("Authorization"))
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "success"}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	mockConf := &mockConfigManager{}

	reqInterface := NewRequestInterface(logs.NewGen3Logger(logger, "", ""), nil, mockConf, server.URL, "test-ua", nil)
	req := reqInterface.(*Request)

	builder := req.New("GET", "/api/test")
	builder = builder.WithToken("test-token")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := req.Do(ctx, builder)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected non-nil response")
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "success") {
		t.Error("Expected response body to contain 'success'")
	}
}

func TestRequest_Do_WithCustomHeaders(t *testing.T) {
	// Create a test server that checks for custom headers
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHeader := r.Header.Get("X-Custom-Header")
		if customHeader != "test-value" {
			t.Errorf("Expected X-Custom-Header 'test-value', got '%s'", customHeader)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	mockConf := &mockConfigManager{}

	reqInterface := NewRequestInterface(logs.NewGen3Logger(logger, "", ""), nil, mockConf, server.URL, "test-ua", nil)
	req := reqInterface.(*Request)

	builder := req.New("GET", "/api/test")
	builder = builder.WithHeader("X-Custom-Header", "test-value")

	ctx := context.Background()
	resp, err := req.Do(ctx, builder)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	resp.Body.Close()
}

// Mock config manager for testing
type mockConfigManager struct{}

func (m *mockConfigManager) Import(filePath, fenceToken string) (*conf.Credential, error) {
	return &conf.Credential{}, nil
}

func (m *mockConfigManager) Load(profile string) (*conf.Credential, error) {
	return &conf.Credential{}, nil
}

func (m *mockConfigManager) Save(cred *conf.Credential) error {
	return nil
}

func (m *mockConfigManager) EnsureExists() error {
	return nil
}

func (m *mockConfigManager) IsCredentialValid(cred *conf.Credential) (bool, error) {
	return true, nil
}

func (m *mockConfigManager) IsTokenValid(token string) (bool, error) {
	return true, nil
}
