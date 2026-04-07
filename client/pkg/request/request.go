package request

//go:generate mockgen -destination=../mocks/mock_request.go -package=mocks github.com/calypr/syfon/client/pkg/request RequestInterface

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/calypr/syfon/client/conf"
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/hashicorp/go-retryablehttp"
)

type Request struct {
	Logs        *logs.Gen3Logger
	RetryClient *retryablehttp.Client
}

type RequestInterface interface {
	New(method, url string) *RequestBuilder
	Do(ctx context.Context, req *RequestBuilder) (*http.Response, error)
}

func NewRequestInterface(
	logger *logs.Gen3Logger,
	cred *conf.Credential,
	conf conf.ManagerInterface,
) RequestInterface {
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = 5
	retryClient.Logger = logger
	retryClient.RetryWaitMin = 5 * time.Second
	retryClient.RetryWaitMax = 15 * time.Second
	baseTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		TLSHandshakeTimeout:   30 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
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

	return &Request{
		RetryClient: retryClient,
		Logs:        logger,
	}
}

func (r *Request) Do(ctx context.Context, rb *RequestBuilder) (*http.Response, error) {
	// Prepare body reader

	httpReq, err := http.NewRequestWithContext(ctx, rb.Method, rb.Url, rb.Body)
	if err != nil {
		return nil, errors.New("failed to create HTTP request: " + err.Error())
	}

	for key, value := range rb.Headers {
		httpReq.Header.Add(key, value)
	}

	if rb.SkipAuth {
		httpReq.Header.Set("X-Skip-Auth", "true")
	}

	if rb.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+rb.Token)
	}

	if rb.PartSize != 0 {
		httpReq.ContentLength = rb.PartSize
	}
	// Convert to retryablehttp.Request
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
