package main

import (
	"flag"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// main is the entry point for the CLI tool.
// It:
//
//	1\) Parses the `-file` flag for the OpenAPI YAML path.
//	2\) Reads the YAML file into memory.
//	3\) Unmarshals the YAML into a generic `any` structure.
//	4\) Recursively removes all `example` keys from the structure.
//	5\) Marshals the modified structure back to YAML.
//	6\) Writes the result back to the same file.
func main() {
	// Define a `-file` flag with a default path to the OpenAPI spec.
	// `path` is a pointer to a string that will hold the flag value.
	path := flag.String("file", "apigen/openapi/openapi.yaml", "path to OpenAPI YAML file")

	// Parse command\-line flags. After this call, `*path` contains the final value.
	flag.Parse()

	// Read the entire contents of the YAML file at `*path`.
	data, err := os.ReadFile(*path)
	if err != nil {
		// On read error, print a message to stderr and exit with non\-zero code.
		fmt.Fprintf(os.Stderr, "read error: %v\n", err)
		os.Exit(1)
	}

	// v will hold the unmarshaled YAML as a tree composed of maps, slices, and scalars.
	var v any

	// Unmarshal the YAML bytes into `v`. The yaml package will choose appropriate Go
	// types (e.g. map\[string\]any, []any, string, float64, etc.).
	if err := yaml.Unmarshal(data, &v); err != nil {
		// On unmarshal error, print a message to stderr and exit non\-zero.
		fmt.Fprintf(os.Stderr, "unmarshal error: %v\n", err)
		os.Exit(1)
	}

	// Recursively walk the structure and remove all `example` fields.
	cleanExamples(v)

	// Marshal the modified structure back into YAML bytes.
	out, err := yaml.Marshal(v)
	if err != nil {
		// On marshal error, print a message to stderr and exit non\-zero.
		fmt.Fprintf(os.Stderr, "marshal error: %v\n", err)
		os.Exit(1)
	}

	// Write the updated YAML back to the same file, using permissions 0o644.
	if err := os.WriteFile(*path, out, 0o644); err != nil {
		// On write error, print a message to stderr and exit non\-zero.
		fmt.Fprintf(os.Stderr, "write error: %v\n", err)
		os.Exit(1)
	}
}

// cleanExamples walks an arbitrary YAML\-derived structure and removes any
// key named `example` from all maps it encounters.
//
// The structure `v` is expected to be composed of:
//   - map\[string\]any for YAML mappings,
//   - []any for YAML sequences,
//   - scalar types (string, float64, bool, etc.) for YAML scalars.
//
// The function modifies the structure in place.
func cleanExamples(v any) {
	switch node := v.(type) {
	case map[string]any:
		// If this map has an `example` key, remove it.
		delete(node, "example")

		// Recurse into all values of the map so nested `example` keys are removed too.
		for _, child := range node {
			cleanExamples(child)
		}

	case []any:
		// For sequences, recurse into each element so we also clean maps inside lists.
		for _, item := range node {
			cleanExamples(item)
		}

		// Other types (scalars) are ignored because they cannot contain nested keys.
	}
}
