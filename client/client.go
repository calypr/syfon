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

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/request"
	"github.com/calypr/syfon/client/transfer"
	"github.com/hashicorp/go-retryablehttp"
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

	// Requestor allows overriding the default HTTP backend.
	Requestor request.RequestInterface
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
	requestor  request.RequestInterface

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

func WithRequestor(r request.RequestInterface) Option {
	return func(c *Config) {
		c.Requestor = r
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

	req := cfg.Requestor
	if req == nil {
		// Default requestor if not provided.
		rClient := retryablehttp.NewClient()
		rClient.Logger = nil // Disable verbose retry logging by default
		req = &request.Request{
			RetryClient: rClient,
		}
	}

	client := &Client{
		baseURL:    strings.TrimRight(u.String(), "/"),
		httpClient: hc,
		userAgent:  ua,
		basicAuth:  cfg.BasicAuth,
		token:      strings.TrimSpace(cfg.Token),
		requestor:  req,
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

// --- Managed Transfer Orchestration ---

// Resolve implements transfer.Resolver by fetching object metadata from the DRS API.
func (c *Client) Resolve(ctx context.Context, id string) (*transfer.ResolvedObject, error) {
	obj, err := c.DRS().GetObject(ctx, id)
	if err != nil {
		return nil, err
	}
	// Convert DRSObject to transfer.ResolvedObject.
	// For simplicity, we assume the first access method is the primary one.
	if len(obj.AccessMethods) == 0 {
		return nil, fmt.Errorf("no access methods found for object %s", id)
	}
	am := obj.AccessMethods[0]
	return &transfer.ResolvedObject{
		Id:           obj.Id,
		Name:         obj.Name,
		Size:         obj.Size,
		ProviderURL:  am.AccessUrl.Url,
		AccessMethod: am.Type,
	}, nil
}

// InitMultipartUpload implements transfer.MultipartURLSigner.
func (c *Client) InitMultipartUpload(ctx context.Context, guid, filename, bucket string) (*common.MultipartUploadInit, error) {
	req := MultipartInitRequest{}
	(&req).SetGuid(guid)
	(&req).SetFileName(filename)
	(&req).SetBucket(bucket)
	resp, err := c.Data().MultipartInit(ctx, req)
	if err != nil {
		return nil, err
	}
	return &common.MultipartUploadInit{
		UploadID: (&resp).GetUploadId(),
		GUID:     (&resp).GetGuid(),
	}, nil
}

// GetMultipartUploadURL implements transfer.MultipartURLSigner.
func (c *Client) GetMultipartUploadURL(ctx context.Context, key, uploadID string, partNum int32, bucket string) (string, error) {
	req := MultipartUploadRequest{}
	(&req).SetKey(key)
	(&req).SetUploadId(uploadID)
	(&req).SetPartNumber(partNum)
	(&req).SetBucket(bucket)
	resp, err := c.Data().MultipartUpload(ctx, req)
	if err != nil {
		return "", err
	}
	return (&resp).GetPresignedUrl(), nil
}

// CompleteMultipartUpload implements transfer.MultipartURLSigner.
func (c *Client) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []common.MultipartUploadPart, bucket string) error {
	var apiParts []MultipartPart
	for _, p := range parts {
		item := MultipartPart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		}
		apiParts = append(apiParts, item)
	}

	req := MultipartCompleteRequest{}
	(&req).SetKey(key)
	(&req).SetUploadId(uploadID)
	(&req).SetBucket(bucket)
	(&req).SetParts(apiParts)
	return c.Data().MultipartComplete(ctx, req)
}

// GetWriter implements transfer.ObjectWriter.
func (c *Client) GetWriter(ctx context.Context, guid string) (io.WriteCloser, error) {
	// For simple single-stream PUT directly to a signed URL.
	req := UploadBlankRequest{}
	(&req).SetGuid(guid)
	_, err := c.Data().UploadBlank(ctx, req)
	if err != nil {
		return nil, err
	}

	// This is a bit tricky as GetWriter expects to return a WriteCloser that doesn't exist yet.
	// We might need a bridge here, but for now we'll assume orchestrated transfer handles it.
	return nil, fmt.Errorf("GetWriter not yet fully implemented for Client; use high-level Upload")
}

// Upload is the high-level orchestrated entry point for data movement.
func (c *Client) Upload(ctx context.Context, req common.FileUploadRequestObject, showProgress bool, opts ...transfer.UploadOptions) error {
	// We pass 'c' as the resolver and signer since it implements the necessary interfaces.
	// We pass 'c.requestor' for the actual HTTP PUT operations to presigned URLs.
	return transfer.Upload(ctx, c, c, req, showProgress, opts...)
}

// New implements request.RequestInterface proxy.
func (c *Client) New(method, url string) *request.RequestBuilder {
	return c.requestor.New(method, url)
}

// Do implements request.RequestInterface proxy.
func (c *Client) Do(ctx context.Context, rb *request.RequestBuilder) (*http.Response, error) {
	return c.requestor.Do(ctx, rb)
}

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
