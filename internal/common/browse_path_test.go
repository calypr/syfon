package common

import "testing"

func TestBrowsePathInfoFromName(t *testing.T) {
	info, ok, err := BrowsePathInfoFromName(` nested\\dir//file.txt `)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected valid browse path info")
	}
	if info.Normalized != "nested/dir/file.txt" {
		t.Fatalf("unexpected normalized path: %q", info.Normalized)
	}
	if info.ParentPath != "nested/dir" {
		t.Fatalf("unexpected parent path: %q", info.ParentPath)
	}
	if info.EntryName != "file.txt" {
		t.Fatalf("unexpected entry name: %q", info.EntryName)
	}
}

func TestNormalizeBrowsePathRejectsDotSegments(t *testing.T) {
	if _, _, err := NormalizeBrowsePath("../nested"); err == nil {
		t.Fatal("expected invalid path error")
	}
}

func TestImmediateBrowseDirectory(t *testing.T) {
	dir, ok := ImmediateBrowseDirectory("nested", "nested/deep/file.txt")
	if !ok {
		t.Fatal("expected directory to be derived")
	}
	if dir.Normalized != "nested/deep" {
		t.Fatalf("unexpected directory path: %q", dir.Normalized)
	}
	if dir.EntryName != "deep" {
		t.Fatalf("unexpected directory name: %q", dir.EntryName)
	}
}
