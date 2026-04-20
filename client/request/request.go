package request

//go:generate mockgen -destination=../mocks/mock_request.go -package=mocks github.com/calypr/syfon/client/request RequestInterface

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/calypr/syfon/client/conf"
	"github.com/calypr/syfon/client/logs"
	"github.com/hashicorp/go-retryablehttp"
)

type Request struct {
	Logs        *logs.Gen3Logger
	RetryClient *retryablehttp.Client

	BaseURL   string
	UserAgent string
	Token     string
	User      string
	Pass      string
}

type ResponseError struct {
	Method  string
	URL     string
	Status  int
	Body    string
	Headers http.Header
}

func (e *ResponseError) Error() string {
	return fmt.Sprintf("%s %s: status %d body=%s", e.Method, e.URL, e.Status, e.Body)
}

type RequestOption func(*RequestBuilder)

func WithQuery(key, value string) RequestOption {
	return func(rb *RequestBuilder) {
		rb.WithQuery(key, value)
	}
}

func WithQueryValues(v url.Values) RequestOption {
	return func(rb *RequestBuilder) {
		rb.WithQueryValues(v)
	}
}

func WithHeader(key, value string) RequestOption {
	return func(rb *RequestBuilder) {
		rb.WithHeader(key, value)
	}
}

func WithTimeout(d time.Duration) RequestOption {
	return func(rb *RequestBuilder) {
		rb.WithTimeout(d)
	}
}

func WithToken(token string) RequestOption {
	return func(rb *RequestBuilder) {
		rb.WithToken(token)
	}
}

func WithSkipAuth(skip bool) RequestOption {
	return func(rb *RequestBuilder) {
		rb.WithSkipAuth(skip)
	}
}

func WithPartSize(size int64) RequestOption {
	return func(rb *RequestBuilder) {
		rb.PartSize = size
	}
}

type Requester interface {
	Do(ctx context.Context, method, path string, body, out any, opts ...RequestOption) error
}

func NewRequestor(
	logger *logs.Gen3Logger,
	cred *conf.Credential,
	conf conf.ManagerInterface,
	baseURL string,
	userAgent string,
	baseHTTPClient *http.Client,
) Requester {
	if logger == nil {
		logger = logs.NewGen3Logger(nil, "", "")
	}
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = 5
	retryClient.Logger = logger
	retryClient.RetryWaitMin = 5 * time.Second
	retryClient.RetryWaitMax = 15 * time.Second

	var baseTransport http.RoundTripper
	if baseHTTPClient != nil && baseHTTPClient.Transport != nil {
		baseTransport = baseHTTPClient.Transport
	} else {
		baseTransport = &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   100,
			TLSHandshakeTimeout:   30 * time.Second,
			ResponseHeaderTimeout: 60 * time.Second,
		}
	}

	authTransport := &AuthTransport{
		Base:    baseTransport,
		Cred:    cred,
		Manager: conf,
	}
	retryClient.HTTPClient = &http.Client{
		Timeout:   0,
		Transport: authTransport,
	}
	if baseHTTPClient != nil {
		retryClient.HTTPClient.Timeout = baseHTTPClient.Timeout
	}

	retryClient.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		shouldRetry, retryErr :=
			retryablehttp.DefaultRetryPolicy(ctx, resp, err)

		if resp != nil &&
			(resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusBadGateway) {
			err := authTransport.refreshOnce(ctx)
			if err != nil {
				return false, err
			}
			return true, nil
		}
		return shouldRetry, retryErr
	}

	r := &Request{
		RetryClient: retryClient,
		Logs:        logger,
		BaseURL:     strings.TrimRight(baseURL, "/"),
		UserAgent:   userAgent,
	}
	if cred != nil {
		r.Token = cred.AccessToken
		r.User = cred.KeyID
		r.Pass = cred.APIKey
	}
	return r
}

func (r *Request) Do(ctx context.Context, method, path string, body, out any, opts ...RequestOption) error {
	rb := r.newBuilder(method, path)
	if body != nil {
		if reader, ok := body.(io.Reader); ok {
			rb.WithBody(reader)
		} else {
			if _, err := rb.WithJSONBody(body); err != nil {
				return err
			}
		}
	}

	for _, opt := range opts {
		opt(rb)
	}

	if rb.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, rb.Timeout)
		defer cancel()
	}

	httpReq, err := http.NewRequestWithContext(ctx, rb.Method, rb.Url, rb.Body)
	if err != nil {
		return errors.New("failed to create HTTP request: " + err.Error())
	}

	// Apply default headers
	if r.UserAgent != "" {
		httpReq.Header.Set("User-Agent", r.UserAgent)
	}
	httpReq.Header.Set("Accept", "application/json")

	// Apply specific headers from builder
	for key, value := range rb.Headers {
		httpReq.Header.Set(key, value)
	}

	if rb.SkipAuth {
		httpReq.Header.Set("X-Skip-Auth", "true")
	}

	// Apply Auth
	token := rb.Token
	if token == "" {
		token = r.Token
	}
	if token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+token)
	} else if r.User != "" {
		auth := base64.StdEncoding.EncodeToString([]byte(r.User + ":" + r.Pass))
		httpReq.Header.Set("Authorization", "Basic "+auth)
	}

	if rb.PartSize != 0 {
		httpReq.ContentLength = rb.PartSize
	}

	retryReq, err := retryablehttp.FromRequest(httpReq)
	if err != nil {
		return err
	}

	resp, err := r.RetryClient.Do(retryReq)
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		return errors.New("request failed after retries: " + err.Error())
	}

	// Polymorphic Response Handling
	switch v := out.(type) {
	case **http.Response:
		// Raw Mode: Caller is responsible for closing the body.
		*v = resp
		return nil

	default:
		// JSON/Void Mode: We close the body.
		defer resp.Body.Close()

		data, _ := io.ReadAll(resp.Body)
		if resp.StatusCode >= 400 {
			return &ResponseError{
				Method:  method,
				URL:     resp.Request.URL.String(),
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
}

func (r *Request) newBuilder(method, path string) *RequestBuilder {
	fullURL := path
	if !strings.HasPrefix(path, "http") {
		fullURL = r.BaseURL + "/" + strings.TrimLeft(path, "/")
	}
	return &RequestBuilder{
		Method:  method,
		Url:     fullURL,
		Headers: make(map[string]string),
	}
}
