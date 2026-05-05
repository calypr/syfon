package common

import "testing"

func TestParseResourcePath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want ResourceScope
	}{
		{
			name: "full scope",
			path: "/organization/cbds/project/proj",
			want: ResourceScope{Organization: "cbds", Project: "proj"},
		},
		{
			name: "organization only",
			path: "/organization/cbds",
			want: ResourceScope{Organization: "cbds"},
		},
		{
			name: "organization alias full scope",
			path: "/organization/cbds/project/proj",
			want: ResourceScope{Organization: "cbds", Project: "proj"},
		},
		{
			name: "organization alias organization only",
			path: "/organization/cbds",
			want: ResourceScope{Organization: "cbds"},
		},
		{
			name: "invalid root",
			path: "/foo/bar",
			want: ResourceScope{},
		},
		{
			name: "trimmed path",
			path: "   /organization/cbds/project/proj  ",
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
