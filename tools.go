//go:build tools
// +build tools

package tools

import (
	_ "github.com/getkin/kin-openapi/openapi3"
	_ "github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen"
	_ "gopkg.in/yaml.v3"
)
