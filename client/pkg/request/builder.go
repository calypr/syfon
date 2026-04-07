package request

import (
	"io"

	"github.com/calypr/syfon/client/pkg/common"
)

// New addition to your request package
type RequestBuilder struct {
	//Req     *Request // the underlying retry client holder
	Method   string
	Url      string
	Body     io.Reader // store as []byte for easy reuse
	Headers  map[string]string
	Token    string
	PartSize int64
	SkipAuth bool
}

func (r *Request) New(method, url string) *RequestBuilder {
	return &RequestBuilder{
		//Req:     r,
		Method:  method,
		Url:     url,
		Headers: make(map[string]string),
	}
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

func (ar *RequestBuilder) WithSkipAuth(skip bool) *RequestBuilder {
	ar.SkipAuth = skip
	return ar
}

func (ar *RequestBuilder) WithPartSize(size int64) *RequestBuilder {
	ar.PartSize = size
	return ar
}
