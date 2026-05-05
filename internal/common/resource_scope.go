package common

import (
	syfoncommon "github.com/calypr/syfon/common"
	"strings"
)

type ResourceScope struct {
	Organization string
	Project      string
}

func ParseResourcePath(path string) ResourceScope {
	normalized := syfoncommon.NormalizeAccessResource(path)
	if !strings.HasPrefix(normalized, "/organization/") {
		return ResourceScope{}
	}
	org, project, ok := syfoncommon.ResourceScope(normalized)
	if !ok {
		return ResourceScope{}
	}
	scope := ResourceScope{Organization: org}
	if project != "" {
		scope.Project = project
	}
	return scope
}
