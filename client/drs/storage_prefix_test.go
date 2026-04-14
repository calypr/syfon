package drs

import "testing"

func TestStoragePrefix(t *testing.T) {
	tests := []struct {
		name    string
		org     string
		project string
		want    string
	}{
		{name: "org + project", org: "cbdsTest", project: "git_drs_e2e_test", want: "cbdsTest/git_drs_e2e_test"},
		{name: "hyphenated project", org: "", project: "prog-proj", want: "prog/proj"},
		{name: "plain project", org: "", project: "projonly", want: "default/projonly"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StoragePrefix(tt.org, tt.project)
			if got != tt.want {
				t.Fatalf("StoragePrefix(%q,%q)=%q want=%q", tt.org, tt.project, got, tt.want)
			}
		})
	}
}
