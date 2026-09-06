package common

import (
	"testing"
)

func TestCleanToBasename(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"Empty", "", ""},
		{"Spaces", "   ", ""},
		{"Unix path", "/foo/bar/baz.txt", "baz.txt"},
		{"Windows path", `C:\foo\bar\baz.txt`, "baz.txt"},
		{"Relative path", "foo/bar.txt", "bar.txt"},
		{"No path", "baz.txt", "baz.txt"},
		{"Slash end", "foo/bar/", "bar"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CleanToBasename(tt.input); got != tt.expected {
				t.Errorf("CleanToBasename(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}
