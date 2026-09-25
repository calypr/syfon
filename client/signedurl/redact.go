package signedurl

import (
	"errors"
	"net/url"
	"strings"
)

type redactedError struct {
	message string
	cause   error
}

func (e *redactedError) Error() string { return e.message }

func (e *redactedError) Unwrap() error { return e.cause }

// Redact removes credentials, query parameters, and fragments from a URL.
func Redact(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		if queryStart := strings.IndexAny(raw, "?#"); queryStart >= 0 {
			return raw[:queryStart]
		}
		return raw
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.RawFragment = ""
	return parsed.String()
}

// RedactError removes the signed URL from the displayed error while preserving
// the error chain, including *url.Error and errors.Join causes.
func RedactError(err error, rawURL string) error {
	redacted, _ := redactErrorChain(err, rawURL)
	return redacted
}

func redactErrorChain(err error, rawURL string) (error, bool) {
	if err == nil {
		return nil, false
	}
	if requestErr, ok := err.(*url.Error); ok {
		redactedCause, causeChanged := redactErrorChain(requestErr.Err, rawURL)
		redactedURL := Redact(requestErr.URL)
		if !causeChanged && redactedURL == requestErr.URL {
			return err, false
		}
		return &url.Error{Op: requestErr.Op, URL: redactedURL, Err: redactedCause}, true
	}

	message := redactErrorText(err.Error(), rawURL)
	if many, ok := err.(interface{ Unwrap() []error }); ok {
		causes := many.Unwrap()
		redactedCauses := make([]error, len(causes))
		changed := message != err.Error()
		for i, cause := range causes {
			var causeChanged bool
			redactedCauses[i], causeChanged = redactErrorChain(cause, rawURL)
			changed = changed || causeChanged
			if causeChanged && cause != nil && redactedCauses[i] != nil {
				message = strings.ReplaceAll(message, cause.Error(), redactedCauses[i].Error())
			}
		}
		if !changed {
			return err, false
		}
		return &redactedError{message: message, cause: errors.Join(redactedCauses...)}, true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		originalCause := wrapped.Unwrap()
		cause, causeChanged := redactErrorChain(originalCause, rawURL)
		if causeChanged && originalCause != nil && cause != nil {
			message = strings.ReplaceAll(message, originalCause.Error(), cause.Error())
		}
		if !causeChanged && message == err.Error() {
			return err, false
		}
		return &redactedError{message: message, cause: cause}, true
	}
	if message != err.Error() {
		return &redactedError{message: message, cause: err}, true
	}
	return err, false
}

func redactErrorText(message, rawURL string) string {
	safeURL := Redact(rawURL)
	for _, candidate := range []string{rawURL, strings.TrimSpace(rawURL)} {
		if candidate != "" {
			message = strings.ReplaceAll(message, candidate, safeURL)
		}
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return message
	}
	message = strings.ReplaceAll(message, parsed.String(), safeURL)
	if parsed.User != nil {
		message = strings.ReplaceAll(message, parsed.User.String(), "[redacted]")
	}
	if parsed.RawQuery != "" {
		message = strings.ReplaceAll(message, parsed.RawQuery, "[redacted]")
	}
	if parsed.Fragment != "" {
		message = strings.ReplaceAll(message, parsed.Fragment, "[redacted]")
	}
	return message
}
