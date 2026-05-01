package common

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNotFound     = errors.New("not found")
	ErrUnauthorized = errors.New("unauthorized")
	ErrConflict     = errors.New("conflict")
)

type PublicError interface {
	PublicMessage() string
}

type AuthorizationError struct {
	Method             string
	RecordID           string
	Resources          []string
	DeniedRecords      int
	TotalRecords       int
	TruncatedResources int
}

func (e *AuthorizationError) Error() string {
	return e.PublicMessage()
}

func (e *AuthorizationError) Unwrap() error {
	return ErrUnauthorized
}

func (e *AuthorizationError) PublicMessage() string {
	if e == nil {
		return "Unauthorized"
	}
	method := strings.TrimSpace(e.Method)
	if method == "" {
		method = "requested"
	}

	denied := e.DeniedRecords
	if denied <= 0 {
		denied = 1
	}
	total := e.TotalRecords
	if total < denied {
		total = denied
	}

	var b strings.Builder
	if total > 1 {
		fmt.Fprintf(&b, "Unauthorized: missing %s access for %d/%d records", method, denied, total)
	} else {
		fmt.Fprintf(&b, "Unauthorized: missing %s access", method)
	}
	if strings.TrimSpace(e.RecordID) != "" {
		fmt.Fprintf(&b, "; first denied record=%q", e.RecordID)
	}
	if len(e.Resources) > 0 {
		fmt.Fprintf(&b, "; denied organization/project scopes: %s", strings.Join(formatResourceScopes(e.Resources), ", "))
		if e.TruncatedResources > 0 {
			fmt.Fprintf(&b, " (and %d more)", e.TruncatedResources)
		}
	}
	return b.String()
}

func formatResourceScopes(resources []string) []string {
	out := make([]string, 0, len(resources))
	for _, resource := range resources {
		scope := ParseResourcePath(resource)
		switch {
		case scope.Organization != "" && scope.Project != "":
			out = append(out, scope.Organization+"/"+scope.Project)
		case scope.Organization != "":
			out = append(out, scope.Organization+"/*")
		default:
			out = append(out, resource)
		}
	}
	return out
}

func IsNotFoundError(err error) bool {
	return errors.Is(err, ErrNotFound)
}

func IsUnauthorizedError(err error) bool {
	return errors.Is(err, ErrUnauthorized)
}
