package api

import (
	"embed"
	"io/fs"
)

// specFS embeds OpenAPI specs so docs endpoints do not depend on runtime filesystem layout.
//
//go:embed *.yaml
var specFS embed.FS

// ReadSpec returns embedded OpenAPI YAML bytes for a given file name.
func ReadSpec(name string) ([]byte, error) {
	return fs.ReadFile(specFS, name)
}
