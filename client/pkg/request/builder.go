package request

import (
	"io"
	"net/url"
	"strings"

	"github.com/calypr/syfon/client/pkg/common"
)

type RequestBuilder struct {
	Method   string
	Url      string
	Body     io.Reader
	Headers  map[string]string
	Token    string
	PartSize int64
	SkipAuth bool
}

func (ar *RequestBuilder) WithToken(token string) *RequestBuilder {
	ar.Token = token
	return ar
}

func (ar *RequestBuilder) WithJSONBody(v any) (*RequestBuilder, error) {
	reader, err := common.ToJSONReader(v)
	if err != nil {
		return nil, err
	}

	ar.Body = reader
	ar.Headers[common.HeaderContentType] = common.MIMEApplicationJSON
	return ar, nil
}

func (ar *RequestBuilder) WithBody(body io.Reader) *RequestBuilder {
	ar.Body = body
	return ar
}

func (ar *RequestBuilder) WithHeader(key, value string) *RequestBuilder {
	ar.Headers[key] = value
	return ar
}

func (ar *RequestBuilder) WithQuery(key, value string) *RequestBuilder {
	if strings.Contains(ar.Url, "?") {
		ar.Url += "&" + url.QueryEscape(key) + "=" + url.QueryEscape(value)
	} else {
		ar.Url += "?" + url.QueryEscape(key) + "=" + url.QueryEscape(value)
	}
	return ar
}

func (ar *RequestBuilder) WithQueryValues(v url.Values) *RequestBuilder {
	if len(v) == 0 {
		return ar
	}
	if strings.Contains(ar.Url, "?") {
		ar.Url += "&" + v.Encode()
	} else {
		ar.Url += "?" + v.Encode()
	}
	return ar
}

func (ar *RequestBuilder) WithSkipAuth(skip bool) *RequestBuilder {
	ar.SkipAuth = skip
	return ar
}

func (ar *RequestBuilder) WithPartSize(size int64) *RequestBuilder {
	ar.PartSize = size
	return ar
}
