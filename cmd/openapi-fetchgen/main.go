package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

var ghBlob = regexp.MustCompile(`^https://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.*)$`)

func main() {
	url := flag.String("url", "", "GitHub URL (blob or raw) to OpenAPI YAML")
	out := flag.String("out", "openapi/openapi.yaml", "output path")
	withRefs := flag.Bool("with-refs", false, "if url is github.com/.../blob/... download repo archive at ref and extract referenced files")
	flag.Parse()

	if *url == "" {
		fmt.Fprintln(os.Stderr, "usage: openapi-fetchgen -url https://github.com/.../blob/.../openapi.yaml [-out openapi/openapi.yaml] [-with-refs]")
		os.Exit(2)
	}

	if *withRefs {
		if err := fetchWithRefs(*url, *out); err != nil {
			fatal(err)
		}
		return
	}

	raw := toRawGitHubURL(*url)
	b, err := fetch(raw)
	if err != nil {
		fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fatal(err)
	}
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		fatal(err)
	}
	fmt.Printf("wrote %s from %s\n", *out, raw)
}

// fetchWithRefs fetches the root blob via raw.githubusercontent.com and then
// scans the YAML for relative $ref values and fetches those files individually.
// Extracted files are written under a temporary dir and then copied to the
// destination directory (preserving relative layout).
func fetchWithRefs(blobURL, out string) error {
	fmt.Fprintln(os.Stderr, "fetching from GitHub...", blobURL)
	m := ghBlob.FindStringSubmatch(strings.TrimSpace(blobURL))
	if len(m) == 0 {
		// fallback: treat as a raw URL or non-GitHub URL
		raw := toRawGitHubURL(blobURL)
		b, err := fetch(raw)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		return os.WriteFile(out, b, 0o644)
	}
	owner, repo, ref, repoPath := m[1], m[2], m[3], m[4]

	// Fetch the root file from raw.githubusercontent.com
	rootRaw := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/%s", owner, repo, url.PathEscape(ref), repoPath)
	rootBytes, err := fetch(rootRaw)
	if err != nil {
		return err
	}

	tmpRoot := filepath.Join(filepath.Dir(out), ".tmp_openapi_extract")
	_ = os.RemoveAll(tmpRoot)
	if err := os.MkdirAll(tmpRoot, 0o755); err != nil {
		return err
	}

	// Write the root file into the temp tree
	rootOut := filepath.Join(tmpRoot, filepath.FromSlash(repoPath))
	if err := os.MkdirAll(filepath.Dir(rootOut), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(rootOut, rootBytes, 0o644); err != nil {
		return err
	}

	// Find relative $ref entries in the root YAML
	refRe := regexp.MustCompile(`\$ref:\s*["']?([^"'\s]+)["']?`)
	matches := refRe.FindAllSubmatch(rootBytes, -1)
	baseDir := path.Dir(repoPath) // use path (forward-slash) for repo paths
	for _, mm := range matches {
		refPath := string(mm[1])
		// skip absolute URLs and in-document refs
		if strings.HasPrefix(refPath, "http://") || strings.HasPrefix(refPath, "https://") || strings.HasPrefix(refPath, "#") {
			continue
		}
		// Resolve relative path against the root file directory
		candidate := path.Clean(path.Join(baseDir, refPath))
		rawRefURL := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/%s", owner, repo, url.PathEscape(ref), candidate)
		b, err := fetch(rawRefURL)
		if err != nil {
			return fmt.Errorf("fetching ref %s: %w", rawRefURL, err)
		}
		outPath := filepath.Join(tmpRoot, filepath.FromSlash(candidate))
		if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(outPath, b, 0o644); err != nil {
			return err
		}
	}

	// Copy extracted files into the destination directory
	wantDir := filepath.ToSlash(filepath.Dir(repoPath))
	if wantDir == "." {
		wantDir = ""
	}
	extractedPath := tmpRoot
	if wantDir != "" {
		extractedPath = filepath.Join(tmpRoot, filepath.FromSlash(wantDir))
	}
	if err := copyDir(extractedPath, filepath.Dir(out)); err != nil {
		return err
	}

	// Ensure the root file exists at 'out'
	if _, err := os.Stat(out); err != nil {
		src := filepath.Join(tmpRoot, filepath.FromSlash(repoPath))
		b, rerr := os.ReadFile(src)
		if rerr != nil {
			return fmt.Errorf("expected %s after extraction; got %v", out, err)
		}
		if err := os.WriteFile(out, b, 0o644); err != nil {
			return err
		}
	}

	fmt.Printf("wrote %s (and refs) from %s/%s@%s\n", out, owner, repo, ref)
	return nil
}

func untarSelected(gz []byte, dstRoot, wantDir string) error {
	r, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return err
	}
	defer r.Close()

	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name := h.Name
		// Strip leading "<repo>-<ref>/" prefix
		if idx := strings.Index(name, "/"); idx >= 0 {
			name = name[idx+1:]
		}
		name = filepath.ToSlash(name)

		if wantDir != "" && !strings.HasPrefix(name, wantDir+"/") && name != wantDir {
			continue
		}
		if h.FileInfo().IsDir() {
			continue
		}
		outPath := filepath.Join(dstRoot, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
			return err
		}
		f, err := os.Create(outPath)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, tr); err != nil {
			_ = f.Close()
			return err
		}
		_ = f.Close()
	}
	return nil
}

func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		out := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(out, 0o755)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(out, b, 0o644)
	})
}

func fetch(u string) ([]byte, error) {
	fmt.Fprintln(os.Stderr, "Fetching", u)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	if tok := strings.TrimSpace(os.Getenv("GITHUB_TOKEN")); tok != "" {
		req.Header.Set("Authorization", "token "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf(
			"http %d fetching %s: %s",
			resp.StatusCode,
			u,
			strings.TrimSpace(string(b)),
		)
	}
	// print url of fetched content to stdout
	fmt.Fprintln(os.Stderr, "fetched", u)

	return io.ReadAll(resp.Body)
}

func toRawGitHubURL(u string) string {
	u = strings.TrimSpace(u)
	if strings.Contains(u, "raw.githubusercontent.com") {
		return u
	}
	m := ghBlob.FindStringSubmatch(u)
	if len(m) == 0 {
		return u
	}
	owner, repo, ref, path := m[1], m[2], m[3], m[4]
	return fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/%s", owner, repo, ref, path)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
