package request

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	conf "github.com/calypr/syfon/client/config"
)

type trackingManager struct {
	mockConfigManager
	saved   *conf.Credential
	saveErr error
}

func (m *trackingManager) Save(cred *conf.Credential) error {
	m.saved = cred
	return m.saveErr
}

func TestAuthTransportRoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("skip auth header bypasses auth", func(t *testing.T) {
		base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Header.Get("X-Skip-Auth") != "" {
				t.Fatal("X-Skip-Auth should be removed before sending")
			}
			if req.Header.Get("Authorization") != "" {
				t.Fatal("authorization should not be injected in skip-auth mode")
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header), Request: req}, nil
		})
		transport := &AuthTransport{Base: base, Cred: &conf.Credential{AccessToken: "token"}}
		req, _ := http.NewRequest(http.MethodGet, "https://example.test", nil)
		req.Header.Set("X-Skip-Auth", "true")
		if _, err := transport.RoundTrip(req); err != nil {
			t.Fatalf("RoundTrip returned error: %v", err)
		}
		if req.Header.Get("X-Skip-Auth") != "true" {
			t.Fatal("RoundTrip must preserve skip marker on the caller request")
		}
	})

	t.Run("injects basic auth when absent", func(t *testing.T) {
		base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if got := req.Header.Get("Authorization"); got != "Basic dXNlcjpwYXNz" {
				t.Fatalf("expected basic auth, got %q", got)
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header), Request: req}, nil
		})
		transport := &AuthTransport{Base: base, Mode: AuthModeBasic, Cred: &conf.Credential{KeyID: "user", APIKey: "pass", APIEndpoint: "https://example.test"}}
		req, _ := http.NewRequest(http.MethodGet, "https://example.test", nil)
		if _, err := transport.RoundTrip(req); err != nil {
			t.Fatalf("RoundTrip returned error: %v", err)
		}
	})

	t.Run("injects bearer token when absent", func(t *testing.T) {
		base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if got := req.Header.Get("Authorization"); got != "Bearer tok" {
				t.Fatalf("expected bearer token, got %q", got)
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header), Request: req}, nil
		})
		transport := &AuthTransport{Base: base, Mode: AuthModeBearer, Cred: &conf.Credential{AccessToken: "tok", APIEndpoint: "https://example.test"}}
		req, _ := http.NewRequest(http.MethodGet, "https://example.test", nil)
		if _, err := transport.RoundTrip(req); err != nil {
			t.Fatalf("RoundTrip returned error: %v", err)
		}
	})

	t.Run("preserves caller authorization", func(t *testing.T) {
		base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if got := req.Header.Get("Authorization"); got != "Basic abc" {
				t.Fatalf("expected existing authorization to be preserved, got %q", got)
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header), Request: req}, nil
		})
		transport := &AuthTransport{Base: base, Mode: AuthModeBearer, Cred: &conf.Credential{AccessToken: "tok"}}
		req, _ := http.NewRequest(http.MethodGet, "https://example.test", nil)
		req.Header.Set("Authorization", "Basic abc")
		if _, err := transport.RoundTrip(req); err != nil {
			t.Fatalf("RoundTrip returned error: %v", err)
		}
	})
}

func TestAuthTransportRestrictsAutomaticAuthToTrustedOrigin(t *testing.T) {
	t.Parallel()

	for _, authCase := range []struct {
		name string
		mode AuthMode
		cred *conf.Credential
		want string
	}{
		{name: "basic", mode: AuthModeBasic, cred: &conf.Credential{KeyID: "user", APIKey: "pass", APIEndpoint: "https://source.example/api"}, want: "Basic dXNlcjpwYXNz"},
		{name: "bearer", mode: AuthModeBearer, cred: &conf.Credential{AccessToken: "token", APIEndpoint: "https://source.example/api"}, want: "Bearer token"},
	} {
		t.Run(authCase.name, func(t *testing.T) {
			t.Parallel()

			t.Run("same origin remains authenticated", func(t *testing.T) {
				base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if got := req.Header.Get("Authorization"); got != authCase.want {
						t.Fatalf("same-origin authorization = %q, want %q", got, authCase.want)
					}
					return testAuthResponse(req, http.StatusOK, nil), nil
				})
				client := &http.Client{Transport: &AuthTransport{Base: base, Mode: authCase.mode, Cred: authCase.cred}}
				resp, err := client.Get("https://SOURCE.example:443/resource")
				if err != nil {
					t.Fatalf("same-origin request failed: %v", err)
				}
				_ = resp.Body.Close()
			})

			t.Run("different origins omit automatic authorization", func(t *testing.T) {
				for _, target := range []string{
					"https://target.example/resource",
					"http://source.example/resource",
					"https://source.example:444/resource",
				} {
					t.Run(target, func(t *testing.T) {
						base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
							if got := req.Header.Get("Authorization"); got != "" {
								t.Fatalf("authorization for %s = %q, want empty", target, got)
							}
							return testAuthResponse(req, http.StatusOK, nil), nil
						})
						client := &http.Client{Transport: &AuthTransport{Base: base, Mode: authCase.mode, Cred: authCase.cred}}
						resp, err := client.Get(target)
						if err != nil {
							t.Fatalf("request to %s failed: %v", target, err)
						}
						_ = resp.Body.Close()
					})
				}
			})

			t.Run("cross-host redirect omits automatic authorization", func(t *testing.T) {
				var calls int
				base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
					calls++
					switch calls {
					case 1:
						if got := req.Header.Get("Authorization"); got != authCase.want {
							t.Fatalf("source authorization = %q, want %q", got, authCase.want)
						}
						return testAuthResponse(req, http.StatusFound, http.Header{"Location": []string{"https://target.example/resource"}}), nil
					case 2:
						if got := req.Header.Get("Authorization"); got != "" {
							t.Fatalf("redirect target authorization = %q, want empty", got)
						}
						return testAuthResponse(req, http.StatusOK, nil), nil
					default:
						t.Fatalf("unexpected request %d to %s", calls, req.URL)
						return nil, nil
					}
				})
				client := &http.Client{Transport: &AuthTransport{Base: base, Mode: authCase.mode, Cred: authCase.cred}}
				resp, err := client.Get("https://source.example/resource")
				if err != nil {
					t.Fatalf("redirected request failed: %v", err)
				}
				_ = resp.Body.Close()
				if calls != 2 {
					t.Fatalf("request count = %d, want 2", calls)
				}
			})
		})
	}

	t.Run("automatic auth is off without trusted origin", func(t *testing.T) {
		base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if got := req.Header.Get("Authorization"); got != "" {
				t.Fatalf("authorization without trusted origin = %q, want empty", got)
			}
			return testAuthResponse(req, http.StatusOK, nil), nil
		})
		transport := &AuthTransport{Base: base, Mode: AuthModeBearer, Cred: &conf.Credential{AccessToken: "token"}}
		resp, err := (&http.Client{Transport: transport}).Get("https://source.example/resource")
		if err != nil {
			t.Fatalf("request without trusted origin failed: %v", err)
		}
		_ = resp.Body.Close()
	})

	t.Run("explicit caller authorization is preserved on foreign URL", func(t *testing.T) {
		base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if got := req.Header.Get("Authorization"); got != "Bearer caller-token" {
				t.Fatalf("caller authorization = %q, want Bearer caller-token", got)
			}
			return testAuthResponse(req, http.StatusOK, nil), nil
		})
		transport := &AuthTransport{Base: base, Mode: AuthModeBearer, Cred: &conf.Credential{AccessToken: "managed-token", APIEndpoint: "https://source.example"}}
		req, err := http.NewRequest(http.MethodGet, "https://target.example/resource", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer caller-token")
		resp, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("foreign request with caller authorization failed: %v", err)
		}
		_ = resp.Body.Close()
	})
}

func testAuthResponse(req *http.Request, status int, header http.Header) *http.Response {
	if header == nil {
		header = make(http.Header)
	}
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     header,
		Request:    req,
	}
}

func TestRequestDoRefreshesRejectedBearerOnceForConcurrent401s(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	refreshCalls := 0
	dataCalls := 0
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		if req.URL.Path == "/user/credentials/api/access_token" {
			refreshCalls++
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"access_token":"new-token"}`)), Header: make(http.Header), Request: req}, nil
		}
		dataCalls++
		if req.Header.Get("Authorization") != "Bearer new-token" {
			return &http.Response{StatusCode: http.StatusUnauthorized, Status: "401 Unauthorized", Body: io.NopCloser(strings.NewReader("expired")), Header: make(http.Header), Request: req}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: req}, nil
	})
	cred := &conf.Credential{APIKey: "api-key", APIEndpoint: "https://example.test", AccessToken: "old-token"}
	client := NewClient(nil, cred, &trackingManager{}, "ua", &http.Client{Transport: base}, AuthModeBearer)
	client.retry.RetryWaitMin = 0
	client.retry.RetryWaitMax = 0

	const workers = 8
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.test/data", nil)
			if err == nil {
				var resp *http.Response
				resp, err = client.Do(req)
				if resp != nil && resp.Body != nil {
					_ = resp.Body.Close()
				}
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent request returned error: %v", err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if refreshCalls != 1 {
		t.Fatalf("expected one token refresh, got %d (data calls=%d)", refreshCalls, dataCalls)
	}
	if cred.AccessToken != "new-token" {
		t.Fatalf("expected refreshed token, got %q", cred.AccessToken)
	}
}

func TestRequestDoPreservesExplicitBearerOn401(t *testing.T) {
	var calls int
	var refreshCalls int
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if state, ok := req.Context().Value(authRequestContextKey{}).(authRequestContext); !ok || !state.explicitAuth {
			t.Fatalf("explicit auth context marker missing: state=%+v ok=%v", state, ok)
		}
		if req.URL.Path == "/user/credentials/api/access_token" {
			refreshCalls++
		}
		return &http.Response{StatusCode: http.StatusUnauthorized, Status: "401 Unauthorized", Body: io.NopCloser(strings.NewReader("denied")), Header: make(http.Header), Request: req}, nil
	})
	cred := &conf.Credential{APIKey: "api-key", APIEndpoint: "https://example.test", AccessToken: "managed-token"}
	client := NewClient(nil, cred, &trackingManager{}, "ua", &http.Client{Transport: base}, AuthModeBearer)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.test/data", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer caller-token")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("expected caller authorization response, got %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected caller authorization status 401, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	if calls != 1 || refreshCalls != 0 {
		t.Fatalf("expected one request and no refresh, got requests=%d refreshes=%d", calls, refreshCalls)
	}
}

func TestRequestDoDoesNotRefreshOrRetryForeign401(t *testing.T) {
	var requestCalls int
	var refreshCalls int
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/user/credentials/api/access_token" {
			refreshCalls++
			return testAuthResponse(req, http.StatusOK, nil), nil
		}
		requestCalls++
		if got := req.Header.Get("Authorization"); got != "" {
			t.Fatalf("foreign request authorization = %q, want empty", got)
		}
		return testAuthResponse(req, http.StatusUnauthorized, nil), nil
	})
	cred := &conf.Credential{APIKey: "api-key", APIEndpoint: "https://source.example", AccessToken: "managed-token"}
	client := NewClient(nil, cred, &trackingManager{}, "ua", &http.Client{Transport: base}, AuthModeBearer)
	client.retry.RetryWaitMin = 0
	client.retry.RetryWaitMax = 0
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://target.example/data", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("foreign 401 request returned error: %v", err)
	}
	_ = resp.Body.Close()
	if requestCalls != 1 || refreshCalls != 0 {
		t.Fatalf("foreign 401 caused requests=%d refreshes=%d, want requests=1 refreshes=0", requestCalls, refreshCalls)
	}
	if cred.AccessToken != "managed-token" {
		t.Fatalf("foreign 401 changed managed token to %q", cred.AccessToken)
	}
}

func TestRequestDoRefreshesEmptyBearerAfter401(t *testing.T) {
	var refreshCalls int
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/user/credentials/api/access_token" {
			refreshCalls++
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"access_token":"bootstrapped"}`)), Header: make(http.Header), Request: req}, nil
		}
		if req.Header.Get("Authorization") != "Bearer bootstrapped" {
			return &http.Response{StatusCode: http.StatusUnauthorized, Status: "401 Unauthorized", Body: io.NopCloser(strings.NewReader("missing token")), Header: make(http.Header), Request: req}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: req}, nil
	})
	cred := &conf.Credential{APIKey: "api-key", APIEndpoint: "https://example.test"}
	client := NewClient(nil, cred, &trackingManager{}, "ua", &http.Client{Transport: base}, AuthModeBearer)
	client.retry.RetryWaitMin = 0
	client.retry.RetryWaitMax = 0
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.test/data", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("empty-token request returned error: %v", err)
	}
	_ = resp.Body.Close()
	if refreshCalls != 1 || cred.AccessToken != "bootstrapped" {
		t.Fatalf("expected one bootstrap refresh, got calls=%d token=%q", refreshCalls, cred.AccessToken)
	}
}

func TestAuthTransportRefreshIfCurrent(t *testing.T) {
	t.Parallel()

	transport := &AuthTransport{}
	if err := transport.refreshIfCurrent(context.Background(), ""); err != nil {
		t.Fatalf("refreshIfCurrent with nil cred returned error: %v", err)
	}

	transport = &AuthTransport{Mode: AuthModeBearer, Cred: &conf.Credential{AccessToken: "already-present", APIEndpoint: "https://example.test"}}
	if err := transport.refreshIfCurrent(context.Background(), "old-token"); err != nil {
		t.Fatalf("refreshIfCurrent with changed token returned error: %v", err)
	}

	transport = &AuthTransport{Mode: AuthModeBasic, Cred: &conf.Credential{APIKey: "basic-pass"}}
	if err := transport.refreshIfCurrent(context.Background(), "old-token"); err != nil {
		t.Fatalf("refreshIfCurrent in basic mode should no-op, got error: %v", err)
	}
}

func TestAuthTransportNewAccessToken(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("requires api key", func(t *testing.T) {
		transport := &AuthTransport{Mode: AuthModeBearer, Cred: &conf.Credential{APIEndpoint: "https://example.test"}}
		if err := transport.NewAccessToken(ctx); err == nil || !strings.Contains(err.Error(), "APIKey is required") {
			t.Fatalf("expected APIKey required error, got %v", err)
		}
	})

	t.Run("requires api endpoint", func(t *testing.T) {
		transport := &AuthTransport{Mode: AuthModeBearer, Cred: &conf.Credential{APIKey: "key"}}
		if err := transport.NewAccessToken(ctx); err == nil || !strings.Contains(err.Error(), "APIEndpoint is required") {
			t.Fatalf("expected APIEndpoint required error, got %v", err)
		}
	})

	t.Run("non-200 includes response body", func(t *testing.T) {
		transport := &AuthTransport{
			Mode: AuthModeBearer,
			Base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusUnauthorized, Status: "401 Unauthorized", Body: io.NopCloser(strings.NewReader("nope")), Header: make(http.Header), Request: req}, nil
			}),
			Cred: &conf.Credential{APIKey: "key", APIEndpoint: "https://example.test"},
		}
		if err := transport.NewAccessToken(ctx); err == nil || !strings.Contains(err.Error(), "401 Unauthorized") || !strings.Contains(err.Error(), "body=nope") {
			t.Fatalf("expected detailed non-200 error, got %v", err)
		}
	})

	t.Run("decode failure bubbles up", func(t *testing.T) {
		transport := &AuthTransport{
			Mode: AuthModeBearer,
			Base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("{")), Header: make(http.Header), Request: req}, nil
			}),
			Cred: &conf.Credential{APIKey: "key", APIEndpoint: "https://example.test"},
		}
		if err := transport.NewAccessToken(ctx); err == nil {
			t.Fatal("expected JSON decode error")
		}
	})

	t.Run("success stores refreshed token and persists", func(t *testing.T) {
		mgr := &trackingManager{}
		transport := &AuthTransport{
			Mode: AuthModeBearer,
			Base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodPost {
					t.Fatalf("expected POST refresh request, got %s", req.Method)
				}
				if ct := req.Header.Get("Content-Type"); ct != "application/json" {
					t.Fatalf("expected JSON content type, got %q", ct)
				}
				return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"access_token":"new-token"}`)), Header: make(http.Header), Request: req}, nil
			}),
			Manager: mgr,
			Cred:    &conf.Credential{APIKey: "key", APIEndpoint: "https://example.test", AccessToken: "old-token"},
		}

		if err := transport.NewAccessToken(ctx); err != nil {
			t.Fatalf("NewAccessToken returned error: %v", err)
		}
		if transport.Cred.AccessToken != "new-token" {
			t.Fatalf("expected refreshed token, got %q", transport.Cred.AccessToken)
		}
		if mgr.saved == nil || mgr.saved.AccessToken != "new-token" {
			t.Fatalf("expected credential to be saved, got %+v", mgr.saved)
		}
	})

	t.Run("save failure is returned and token is restored", func(t *testing.T) {
		wantErr := errors.New("save failed")
		mgr := &trackingManager{saveErr: wantErr}
		cred := &conf.Credential{APIKey: "key", APIEndpoint: "https://example.test", AccessToken: "old-token"}
		transport := &AuthTransport{
			Mode:    AuthModeBearer,
			Manager: mgr,
			Base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"access_token":"new-token"}`)), Header: make(http.Header), Request: req}, nil
			}),
			Cred: cred,
		}
		if err := transport.NewAccessToken(ctx); err == nil || !errors.Is(err, wantErr) {
			t.Fatalf("expected save error, got %v", err)
		}
		if cred.AccessToken != "old-token" {
			t.Fatalf("expected old token after save failure, got %q", cred.AccessToken)
		}
	})
}

func TestAuthTransportNewAccessTokenRedirects(t *testing.T) {
	for _, redirect := range []struct {
		name   string
		status int
	}{
		{name: "307", status: http.StatusTemporaryRedirect},
		{name: "308", status: http.StatusPermanentRedirect},
	} {
		t.Run(redirect.name+" cross-origin redirect is rejected", func(t *testing.T) {
			var calls int
			var targetReceivedAPIKey bool
			transport := &AuthTransport{
				Mode: AuthModeBearer,
				Cred: &conf.Credential{APIKey: "synthetic-api-key", APIEndpoint: "https://source.invalid"},
				Base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					calls++
					if req.URL.Host == "target.invalid" {
						body, err := io.ReadAll(req.Body)
						if err != nil {
							return nil, err
						}
						targetReceivedAPIKey = strings.Contains(string(body), "synthetic-api-key")
						return testAuthResponse(req, http.StatusOK, nil), nil
					}
					return testAuthResponse(req, redirect.status, http.Header{
						"Location": []string{"https://target.invalid/redirected"},
					}), nil
				}),
			}

			err := transport.NewAccessToken(context.Background())
			if targetReceivedAPIKey {
				t.Fatal("cross-origin redirect forwarded the refresh API key")
			}
			if calls != 1 {
				t.Fatalf("cross-origin redirect sent %d requests, want 1", calls)
			}
			if err == nil {
				t.Fatal("cross-origin refresh redirect should be rejected")
			}
		})

		t.Run(redirect.name+" same-origin redirect is followed", func(t *testing.T) {
			var calls int
			transport := &AuthTransport{
				Mode: AuthModeBearer,
				Cred: &conf.Credential{APIKey: "synthetic-api-key", APIEndpoint: "https://source.invalid"},
				Base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					calls++
					if calls == 1 {
						return testAuthResponse(req, redirect.status, http.Header{
							"Location": []string{"https://source.invalid/redirected"},
						}), nil
					}
					body, err := io.ReadAll(req.Body)
					if err != nil {
						return nil, err
					}
					if req.Method != http.MethodPost || !strings.Contains(string(body), "synthetic-api-key") {
						t.Fatal("same-origin redirect did not preserve the refresh POST body")
					}
					response := testAuthResponse(req, http.StatusOK, nil)
					response.Body = io.NopCloser(strings.NewReader(`{"access_token":"refreshed"}`))
					return response, nil
				}),
			}

			if err := transport.NewAccessToken(context.Background()); err != nil {
				t.Fatalf("same-origin refresh redirect failed: %v", err)
			}
			if calls != 2 {
				t.Fatalf("same-origin redirect sent %d requests, want 2", calls)
			}
			if transport.Cred.AccessToken != "refreshed" {
				t.Fatal("same-origin redirect did not store the refreshed token")
			}
		})
	}
}

func TestAuthTransportNewAccessTokenUsesConfiguredRedirectPolicy(t *testing.T) {
	var calls int
	var policyCalls int
	base := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls++
			return testAuthResponse(req, http.StatusTemporaryRedirect, http.Header{
				"Location": []string{"https://source.invalid/redirected"},
			}), nil
		}),
		CheckRedirect: func(*http.Request, []*http.Request) error {
			policyCalls++
			return http.ErrUseLastResponse
		},
	}
	client := NewClient(nil, &conf.Credential{APIKey: "synthetic-api-key", APIEndpoint: "https://source.invalid"}, nil, "", base, AuthModeBearer)
	transport := client.standard.Transport.(*AuthTransport)

	if err := transport.NewAccessToken(context.Background()); err == nil {
		t.Fatal("configured redirect policy should stop the refresh redirect")
	}
	if calls != 1 || policyCalls != 1 {
		t.Fatalf("refresh calls=%d redirect policy calls=%d, want 1 each", calls, policyCalls)
	}
}

func TestAuthTransportNewAccessTokenUsesConfiguredTimeout(t *testing.T) {
	deadlineSeen := make(chan bool, 1)
	base := &http.Client{
		Timeout: 50 * time.Millisecond,
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			_, hasDeadline := req.Context().Deadline()
			deadlineSeen <- hasDeadline
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-time.After(500 * time.Millisecond):
				return nil, errors.New("refresh request context had no timeout")
			}
		}),
	}
	client := NewClient(nil, &conf.Credential{APIKey: "synthetic-api-key", APIEndpoint: "https://source.invalid"}, nil, "", base, AuthModeBearer)
	transport := client.standard.Transport.(*AuthTransport)
	err := transport.NewAccessToken(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("refresh did not end at the configured timeout: %v", err)
	}
	if !<-deadlineSeen {
		t.Fatal("configured HTTP client timeout did not add a deadline to the refresh request")
	}
}
