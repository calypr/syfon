package fence

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/calypr/syfon/client/conf"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/request"
)

type Interface interface {
	CheckPrivileges(ctx context.Context) (map[string]any, error)
}

type Client struct {
	request.RequestInterface
	cred   *conf.Credential
	logger *slog.Logger
}

func NewFenceClient(req request.RequestInterface, cred *conf.Credential, logger *slog.Logger) Interface {
	return &Client{
		RequestInterface: req,
		cred:             cred,
		logger:           logger,
	}
}

func (f *Client) CheckPrivileges(ctx context.Context) (map[string]any, error) {
	resp, err := f.Do(ctx, &request.RequestBuilder{
		Url:    f.cred.APIEndpoint + common.FenceUserEndpoint,
		Method: http.MethodGet,
		Token:  f.cred.AccessToken,
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var data map[string]any
	if err := json.Unmarshal(bodyBytes, &data); err != nil {
		return nil, err
	}

	resourceAccess, ok := data["authz"].(map[string]any)
	if len(resourceAccess) == 0 || !ok {
		resourceAccess, ok = data["project_access"].(map[string]any)
		if !ok {
			if f.logger != nil {
				f.logger.Debug("unable to read user privileges from fence response")
			}
			return nil, errors.New("not possible to read access privileges of user")
		}
	}

	return resourceAccess, nil
}
