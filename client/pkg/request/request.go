package request

//go:generate mockgen -destination=../mocks/mock_request.go -package=mocks github.com/calypr/syfon/client/pkg/request RequestInterface

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/syfon/client/conf"
	"github.com/calypr/syfon/client/pkg/logs"
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

type RequestInterface interface {
	New(method, path string) *RequestBuilder
	Do(ctx context.Context, req *RequestBuilder) (*http.Response, error)
	DoJSON(ctx context.Context, req *RequestBuilder, out any) error
}

func NewRequestInterface(
	logger *logs.Gen3Logger,
	cred *conf.Credential,
	conf conf.ManagerInterface,
	baseURL string,
	userAgent string,
	baseHTTPClient *http.Client,
) RequestInterface {
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

func (r *Request) New(method, path string) *RequestBuilder {
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

func (r *Request) Do(ctx context.Context, rb *RequestBuilder) (*http.Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, rb.Method, rb.Url, rb.Body)
	if err != nil {
		return nil, errors.New("failed to create HTTP request: " + err.Error())
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
		return nil, err
	}

	resp, err := r.RetryClient.Do(retryReq)
	if err != nil {
		return resp, errors.New("request failed after retries: " + err.Error())
	}

	return resp, nil
}

func (r *Request) DoJSON(ctx context.Context, rb *RequestBuilder, out any) error {
	resp, err := r.Do(ctx, rb)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return &ResponseError{
			Method:  rb.Method,
			URL:     rb.Url,
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
