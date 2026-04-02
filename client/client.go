package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultAddress = "http://127.0.0.1:8080"
	defaultTimeout = 60 * time.Second
	defaultUA      = "syfon-client/0"
)

type Config struct {
	Address    string
	HTTPClient *http.Client
	UserAgent  string
	BasicAuth  *BasicAuth
	Token      string
}

type BasicAuth struct {
	Username string
	Password string
}

type Client struct {
	baseURL    string
	httpClient *http.Client
	userAgent  string
	basicAuth  *BasicAuth
	token      string

	health  *HealthService
	data    *DataService
	index   *IndexService
	drs     *DRSService
	buckets *BucketsService
	core    *CoreService
	metrics *MetricsService
}

type Option func(*Config)

func WithHTTPClient(h *http.Client) Option {
	return func(c *Config) {
		if h != nil {
			c.HTTPClient = h
		}
	}
}

func WithUserAgent(v string) Option {
	return func(c *Config) {
		if strings.TrimSpace(v) != "" {
			c.UserAgent = strings.TrimSpace(v)
		}
	}
}

func WithBasicAuth(user, pass string) Option {
	return func(c *Config) {
		user = strings.TrimSpace(user)
		if user == "" {
			c.BasicAuth = nil
			return
		}
		c.BasicAuth = &BasicAuth{Username: user, Password: pass}
	}
}

func WithBearerToken(token string) Option {
	return func(c *Config) {
		c.Token = strings.TrimSpace(token)
	}
}

func DefaultConfig() *Config {
	return &Config{
		Address:    defaultAddress,
		HTTPClient: &http.Client{Timeout: defaultTimeout},
		UserAgent:  defaultUA,
	}
}

func New(baseURL string, opts ...Option) *Client {
	cfg := DefaultConfig()
	cfg.Address = baseURL
	for _, opt := range opts {
		opt(cfg)
	}
	c, err := NewClient(cfg)
	if err != nil {
		// Keep constructor ergonomic like Vault's api.NewClient usage pattern.
		panic(err)
	}
	return c
}

func NewClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	base := strings.TrimSpace(cfg.Address)
	if base == "" {
		base = defaultAddress
	}
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("parse address: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid address %q", base)
	}

	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: defaultTimeout}
	}
	ua := strings.TrimSpace(cfg.UserAgent)
	if ua == "" {
		ua = defaultUA
	}

	client := &Client{
		baseURL:    strings.TrimRight(u.String(), "/"),
		httpClient: hc,
		userAgent:  ua,
		basicAuth:  cfg.BasicAuth,
		token:      strings.TrimSpace(cfg.Token),
	}
	client.health = &HealthService{c: client}
	client.data = &DataService{c: client}
	client.index = &IndexService{c: client}
	client.drs = &DRSService{c: client}
	client.buckets = &BucketsService{c: client}
	client.core = &CoreService{c: client}
	client.metrics = &MetricsService{c: client}
	return client, nil
}

func (c *Client) Address() string { return c.baseURL }

func (c *Client) Health() *HealthService   { return c.health }
func (c *Client) Data() *DataService       { return c.data }
func (c *Client) Index() *IndexService     { return c.index }
func (c *Client) DRS() *DRSService         { return c.drs }
func (c *Client) Buckets() *BucketsService { return c.buckets }
func (c *Client) Core() *CoreService       { return c.core }
func (c *Client) Metrics() *MetricsService { return c.metrics }

type ResponseError struct {
	Method  string
	URL     string
	Status  int
	Body    string
	Headers http.Header
}

func (e *ResponseError) Error() string {
	return fmt.Sprintf("%s %s failed: status=%d body=%s", e.Method, e.URL, e.Status, e.Body)
}

func (c *Client) doJSON(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	fullURL := c.baseURL + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.basicAuth != nil {
		req.SetBasicAuth(c.basicAuth.Username, c.basicAuth.Password)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return &ResponseError{
			Method:  method,
			URL:     fullURL,
			Status:  resp.StatusCode,
			Body:    strings.TrimSpace(string(data)),
			Headers: resp.Header.Clone(),
		}
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
