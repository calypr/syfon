package client

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/calypr/syfon/client/conf"
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/calypr/syfon/client/pkg/request"
)

const (
	defaultAddress = "http://127.0.0.1:8080"
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
	requestor request.Requester
	baseURL   string

	health  *HealthService
	data    *DataService
	index   *IndexService
	drs     *DRSService
	buckets *BucketsService
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
		Address: defaultAddress,
		// We set absolute Timeout to 0 to allow per-request context timeouts
		// to control the deadline (essential for large uploads).
		HTTPClient: &http.Client{Timeout: 0},
		UserAgent:  defaultUA,
	}
}

func New(baseURL string, opts ...Option) (*Client, error) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		baseURL = "http://127.0.0.1:8080"
	}
	if !strings.Contains(baseURL, "://") {
		baseURL = "http://" + baseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")

	cfg := DefaultConfig()
	cfg.Address = baseURL
	for _, opt := range opts {
		opt(cfg)
	}
	return NewClient(cfg)
}

func NewClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	bu, err := parseBaseURL(cfg.Address)
	if err != nil {
		return nil, err
	}

	userAgent := strings.TrimSpace(cfg.UserAgent)
	if userAgent == "" {
		userAgent = defaultUA
	}

	// Initialize the hardened requestor
	cred := &conf.Credential{
		AccessToken: cfg.Token,
	}
	if cfg.BasicAuth != nil {
		cred.KeyID = cfg.BasicAuth.Username
		cred.APIKey = cfg.BasicAuth.Password
	}
	req := request.NewRequestor(nil, cred, nil, bu, userAgent, cfg.HTTPClient)

	client := &Client{
		requestor: req,
		baseURL:   bu,
	}
	client.initServices()
	return client, nil
}

func parseBaseURL(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		addr = defaultAddress
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		return "", fmt.Errorf("parse address: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid address %q", addr)
	}
	return strings.TrimRight(u.String(), "/"), nil
}

func (c *Client) initServices() {
	l := c.Logger()
	c.health = NewHealthService(c.requestor)
	c.index = NewIndexService(c.requestor)
	c.drs = NewDRSService(c.requestor, c.index)
	c.data = NewDataService(c.requestor, l, c.drs)
	c.buckets = NewBucketsService(c.requestor)
	c.metrics = NewMetricsService(c.requestor)
}

func (c *Client) Address() string { return c.baseURL }

func (c *Client) Health() *HealthService   { return c.health }
func (c *Client) Data() *DataService       { return c.data }
func (c *Client) Index() *IndexService     { return c.index }
func (c *Client) DRS() *DRSService         { return c.drs }
func (c *Client) Buckets() *BucketsService { return c.buckets }
func (c *Client) Metrics() *MetricsService { return c.metrics }

func (c *Client) Requestor() request.Requester { return c.requestor }

func (c *Client) Logger() *logs.Gen3Logger {
	if r, ok := c.requestor.(*request.Request); ok {
		return r.Logs
	}
	return logs.NewGen3Logger(nil, "", "")
}
