package common

import "strings"

type ResourceScope struct {
	Organization string
	Project      string
}

func ParseResourcePath(path string) ResourceScope {
	path = strings.TrimSpace(path)
	parts := strings.Split(path, "/")
	if len(parts) < 3 || parts[1] != "programs" {
		return ResourceScope{}
	}
	scope := ResourceScope{Organization: parts[2]}
	if len(parts) >= 5 && parts[3] == "projects" {
		scope.Project = parts[4]
	}
	return scope
}
