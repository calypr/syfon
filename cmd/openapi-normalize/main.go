package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Normalizes OpenAPI YAML for oapi-codegen / kin-openapi compatibility.
// Today it focuses on:
//   - tags[].description: { $ref: ./tags/Foo.md }  --> inline file contents as string
//
// This pattern appears in GA4GH DRS schemas and breaks kin-openapi because Tag.description is a string.
func main() {
	in := flag.String("in", "", "input OpenAPI YAML path")
	out := flag.String("out", "", "output YAML path")
	flag.Parse()

	if *in == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: openapi-normalize -in openapi.yaml -out openapi.normalized.yaml")
		os.Exit(2)
	}

	b, err := os.ReadFile(*in)
	if err != nil {
		fatal(err)
	}

	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		fatal(err)
	}

	baseDir := filepath.Dir(*in)
	changed := inlineTagDescriptionRefs(&doc, baseDir)

	outBytes, err := yaml.Marshal(&doc)
	if err != nil {
		fatal(err)
	}

	// Ensure output ends with newline for tooling friendliness
	if !bytes.HasSuffix(outBytes, []byte("\n")) {
		outBytes = append(outBytes, '\n')
	}

	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fatal(err)
	}
	if err := os.WriteFile(*out, outBytes, 0o644); err != nil {
		fatal(err)
	}

	if changed {
		fmt.Fprintf(os.Stderr, "normalized OpenAPI written to %s\n", *out)
	}
}

func inlineTagDescriptionRefs(doc *yaml.Node, baseDir string) bool {
	// Walk mapping to find "tags" key
	var changed bool
	walk(doc, func(n *yaml.Node) {
		if n.Kind != yaml.MappingNode {
			return
		}
		for i := 0; i < len(n.Content)-1; i += 2 {
			k := n.Content[i]
			v := n.Content[i+1]
			if k.Kind == yaml.ScalarNode && k.Value == "tags" && v.Kind == yaml.SequenceNode {
				if inlineInTagsSeq(v, baseDir) {
					changed = true
				}
			}
		}
	})
	return changed
}

func inlineInTagsSeq(seq *yaml.Node, baseDir string) bool {
	changed := false
	for _, item := range seq.Content {
		if item.Kind != yaml.MappingNode {
			continue
		}
		// find description key
		for i := 0; i < len(item.Content)-1; i += 2 {
			k := item.Content[i]
			v := item.Content[i+1]
			if k.Kind == yaml.ScalarNode && k.Value == "description" && v.Kind == yaml.MappingNode {
				ref := findRef(v)
				if ref == "" {
					continue
				}
				path := strings.TrimPrefix(ref, "./")
				abs := filepath.Join(baseDir, filepath.FromSlash(path))
				b, err := os.ReadFile(abs)
				if err != nil {
					// If we can't read it, fall back to empty string rather than failing codegen.
					b = []byte("")
				}
				item.Content[i+1] = &yaml.Node{
					Kind:  yaml.ScalarNode,
					Tag:   "!!str",
					Style: yaml.LiteralStyle,
					Value: string(b),
				}
				changed = true
			}
		}
	}
	return changed
}

func findRef(m *yaml.Node) string {
	if m.Kind != yaml.MappingNode {
		return ""
	}
	for i := 0; i < len(m.Content)-1; i += 2 {
		k := m.Content[i]
		v := m.Content[i+1]
		if k.Kind == yaml.ScalarNode && k.Value == "$ref" && v.Kind == yaml.ScalarNode {
			return v.Value
		}
	}
	return ""
}

func walk(n *yaml.Node, f func(*yaml.Node)) {
	f(n)
	for _, c := range n.Content {
		walk(c, f)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
