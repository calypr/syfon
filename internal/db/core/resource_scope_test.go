package core

import "testing"

func TestResourcePathForScope(t *testing.T) {
	tests := []struct {
		name    string
		org     string
		project string
		want    string
	}{
		{name: "empty org", org: "", project: "p1", want: ""},
		{name: "org only", org: "cbds", project: "", want: "/programs/cbds"},
		{name: "org and project", org: "cbds", project: "proj", want: "/programs/cbds/projects/proj"},
		{name: "trimmed", org: " cbds ", project: " proj ", want: "/programs/cbds/projects/proj"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResourcePathForScope(tc.org, tc.project); got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

func TestParseResourcePath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want ResourceScope
	}{
		{
			name: "full scope",
			path: "/programs/cbds/projects/proj",
			want: ResourceScope{Organization: "cbds", Project: "proj"},
		},
		{
			name: "organization only",
			path: "/programs/cbds",
			want: ResourceScope{Organization: "cbds"},
		},
		{
			name: "invalid root",
			path: "/foo/bar",
			want: ResourceScope{},
		},
		{
			name: "trimmed path",
			path: "   /programs/cbds/projects/proj  ",
			want: ResourceScope{Organization: "cbds", Project: "proj"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseResourcePath(tc.path)
			if got != tc.want {
				t.Fatalf("expected %+v, got %+v", tc.want, got)
			}
		})
	}
}
