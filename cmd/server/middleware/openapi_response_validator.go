package middleware

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/gin-gonic/gin"
)

type ResponseValidationMode string

const (
	ResponseValidationAudit   ResponseValidationMode = "audit"   // log only
	ResponseValidationEnforce ResponseValidationMode = "enforce" // block invalid responses
)

type ResponseValidatorConfig struct {
	Mode             ResponseValidationMode
	MaxBodyBytes     int64 // cap buffering to avoid OOM
	LogHeaders       bool  // include response headers in logs
	RedactHeaders    []string
	AllowEmptyOn204  bool // treat empty body as ok for 204 even if handler wrote nothing
	SkipContentTypes []string
}

func DefaultResponseValidatorConfig() ResponseValidatorConfig {
	return ResponseValidatorConfig{
		Mode:             ResponseValidationAudit,
		MaxBodyBytes:     2 * 1024 * 1024, // 2MiB
		LogHeaders:       false,
		RedactHeaders:    []string{"authorization", "x-api-key", "x-amz-security-token"},
		AllowEmptyOn204:  true,
		SkipContentTypes: []string{"text/event-stream"}, // SSE, etc.
	}
}

// NewOpenAPIResponseValidator validates responses against the OpenAPI operation matched by the request validator.
// Requires the request validator to set:
// - "oapi.route" (routers.Route)
// - "oapi.pathParams" (map[string]string)
func NewOpenAPIResponseValidator(cfg ResponseValidatorConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		log.Printf("OpenAPI response validation middleware started")
		// If the request validator didn't attach route info, skip.
		vRoute, ok := c.Get("oapi.route")
		if !ok {
			log.Printf("Didn't find oapi.route in context; skipping response validation")
			c.Next()
			return
		}
		route, ok := vRoute.(*routers.Route)
		if !ok {
			log.Printf("oapi.route in context has wrong type; skipping response validation")
			c.Next()
			return
		}

		vPP, _ := c.Get("oapi.pathParams")
		pathParams, _ := vPP.(map[string]string)

		// Wrap writer with a buffering, late-commit writer
		bw := newBufferingWriter(c.Writer, cfg.MaxBodyBytes)
		c.Writer = bw

		// Run downstream handlers
		c.Next()

		// If handler aborted with an error and wrote nothing, you may skip validation if desired.
		// Here we still validate what was produced (if anything).
		status := bw.status
		if status == 0 {
			// Gin defaults to 200 if never set explicitly
			status = http.StatusOK
		}

		// Skip streaming / content types you don't want to buffer/validate
		ct := bw.header.Get("Content-Type")
		if shouldSkipContentType(ct, cfg.SkipContentTypes) {
			// Commit without validating
			_ = bw.commit()
			return
		}

		// If response too large to buffer, decide policy
		if bw.tooLarge {
			msg := fmt.Sprintf("response body exceeded max buffer (%d bytes); skipping OpenAPI response validation", cfg.MaxBodyBytes)
			if cfg.Mode == ResponseValidationEnforce {
				// In enforce mode, safest is to fail closed *before committing*.
				// We can still send a 500 because we haven’t committed yet.
				c.Writer = bw.underlying // restore
				c.Status(http.StatusInternalServerError)
				c.Header("Content-Type", "application/json")
				c.Writer.Write([]byte(fmt.Sprintf(`{"error":"openapi response validation failed","detail":%q}`, msg)))
				return
			}
			// audit mode: commit and log
			logOpenAPIResponseIssue(c, cfg, msg, status, bw.header, bw.body.Bytes())
			_ = bw.commit()
			return
		}

		// Validate response using kin-openapi
		bodyReader := bytes.NewReader(bw.body.Bytes())
		respInput := &openapi3filter.ResponseValidationInput{
			RequestValidationInput: &openapi3filter.RequestValidationInput{
				Request:    c.Request,
				PathParams: pathParams,
				Route:      route,
			},
			Status: status,
			Header: cloneHeader(bw.header),
			Body:   io.NopCloser(bodyReader),
		}

		log.Printf("OpenAPI response validation before ValidateResponse")
		if err := openapi3filter.ValidateResponse(c.Request.Context(), respInput); err != nil {
			// Invalid response
			log.Printf("OpenAPI response validation failed: %v", err)
			detail := err.Error()
			logOpenAPIResponseIssue(c, cfg, detail, status, bw.header, bw.body.Bytes())

			if cfg.Mode == ResponseValidationEnforce {
				// Fail closed with a controlled 500, and DO NOT commit the invalid response.
				c.Writer = bw.underlying // restore
				c.Status(http.StatusInternalServerError)
				c.Header("Content-Type", "application/json")
				_, _ = c.Writer.Write([]byte(fmt.Sprintf(`{"error":"openapi response validation failed","detail":%q}`, detail)))
				return
			}
			// audit: commit anyway
		}

		// Commit the buffered response
		_ = bw.commit()
	}
}

func shouldSkipContentType(ct string, skip []string) bool {
	ct = strings.ToLower(strings.TrimSpace(strings.Split(ct, ";")[0]))
	if ct == "" {
		return false
	}
	for _, s := range skip {
		if ct == strings.ToLower(strings.TrimSpace(s)) {
			return true
		}
	}
	return false
}

func cloneHeader(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, v := range h {
		vs := make([]string, len(v))
		copy(vs, v)
		out[k] = vs
	}
	return out
}
