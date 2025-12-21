package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"gopkg.in/yaml.v3"
)

type refEvent struct {
	URI        string `json:"uri"`
	Scheme     string `json:"scheme"`
	Host       string `json:"host,omitempty"`
	Path       string `json:"path,omitempty"`
	Bytes      int    `json:"bytes,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
	OK         bool   `json:"ok"`
	Error      string `json:"error,omitempty"`
	StartedAt  string `json:"startedAt,omitempty"`
	FinishedAt string `json:"finishedAt,omitempty"`
}

type diagReport struct {
	Input         string     `json:"input"`
	Output        string     `json:"output"`
	BaseDir       string     `json:"baseDir"`
	TreeFileCount int        `json:"treeFileCount"`
	TreeByteCount int64      `json:"treeByteCount"`
	TreeTopDirs   []string   `json:"treeTopDirs,omitempty"`
	RefAttempts   []refEvent `json:"refAttempts"`
	RemainingRefs int        `json:"remainingRefs"`
	ValidateError string     `json:"validateError,omitempty"`
	LoadError     string     `json:"loadError,omitempty"`
	Notes         []string   `json:"notes,omitempty"`
}

func main() {
	in := flag.String("in", "", "input OpenAPI YAML path (normalized)")
	out := flag.String("out", "", "output bundled YAML path")
	diag := flag.Bool("diag", true, "emit diagnostics to stderr")
	diagJSON := flag.String("diag-json", "", "if set, write diagnostics report JSON to this path")
	base := flag.String("base", "", "base directory for relative refs (default: dir(in))")
	maxRefBytes := flag.Int("max-ref-bytes", 8<<20, "max bytes to read per referenced file (default 8MiB)")
	flag.Parse()

	if *in == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: openapi-bundle -in openapi.normalized.yaml -out openapi.bundled.yaml [-diag] [-diag-json report.json]")
		os.Exit(2)
	}

	baseDir := *base
	if baseDir == "" {
		baseDir = filepath.Dir(*in)
	}

	rep := &diagReport{
		Input:   *in,
		Output:  *out,
		BaseDir: baseDir,
	}

	// Useful signal that your fetch step actually pulled openapi/{paths,components,...}
	rep.TreeFileCount, rep.TreeByteCount, rep.TreeTopDirs = treeStats(baseDir, 2)

	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true

	var attempts []refEvent

	// NOTE: signature matters: func(*openapi3.Loader, *url.URL) ([]byte, error)
	loader.ReadFromURIFunc = func(l *openapi3.Loader, u *url.URL) ([]byte, error) {
		ev := refEvent{
			URI:       u.String(),
			Scheme:    u.Scheme,
			Host:      u.Host,
			Path:      u.Path,
			StartedAt: time.Now().Format(time.RFC3339Nano),
		}

		b, err := loadURIWithCap(u, baseDir, *maxRefBytes) // <-- deref flag pointer
		if err != nil {
			ev.OK = false
			ev.Error = err.Error()
			ev.FinishedAt = time.Now().Format(time.RFC3339Nano)
			attempts = append(attempts, ev)
			return nil, err
		}

		sum := sha256.Sum256(b)
		ev.OK = true
		ev.Bytes = len(b)
		ev.SHA256 = hex.EncodeToString(sum[:])
		ev.FinishedAt = time.Now().Format(time.RFC3339Nano)
		attempts = append(attempts, ev)
		return b, nil
	}

	spec, err := loader.LoadFromFile(*in)
	if err != nil {
		rep.LoadError = err.Error()
		rep.RefAttempts = attempts
		emitDiagnostics(rep, *diag, *diagJSON)
		os.Exit(1)
	}

	// Warn-only validation (some specs have extensions / partial validation issues)
	if err := spec.Validate(context.Background()); err != nil {
		rep.ValidateError = err.Error()
	}

	j, err := spec.MarshalJSON()
	if err != nil {
		rep.LoadError = "marshal json: " + err.Error()
		rep.RefAttempts = attempts
		emitDiagnostics(rep, *diag, *diagJSON)
		os.Exit(1)
	}

	rep.RemainingRefs = countRefsInJSON(j)

	var y any
	if err := yaml.Unmarshal(j, &y); err != nil {
		rep.LoadError = "json->yaml unmarshal: " + err.Error()
		rep.RefAttempts = attempts
		emitDiagnostics(rep, *diag, *diagJSON)
		os.Exit(1)
	}

	yb, err := yaml.Marshal(y)
	if err != nil {
		rep.LoadError = "yaml marshal: " + err.Error()
		rep.RefAttempts = attempts
		emitDiagnostics(rep, *diag, *diagJSON)
		os.Exit(1)
	}
	if !bytes.HasSuffix(yb, []byte("\n")) {
		yb = append(yb, '\n')
	}

	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		rep.LoadError = "mkdir: " + err.Error()
		rep.RefAttempts = attempts
		emitDiagnostics(rep, *diag, *diagJSON)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, yb, 0o644); err != nil {
		rep.LoadError = "write: " + err.Error()
		rep.RefAttempts = attempts
		emitDiagnostics(rep, *diag, *diagJSON)
		os.Exit(1)
	}

	rep.RefAttempts = attempts
	emitDiagnostics(rep, *diag, *diagJSON)
}

func emitDiagnostics(rep *diagReport, toStderr bool, jsonPath string) {
	if toStderr {
		fmt.Fprintf(os.Stderr, "\n[openapi-bundle diagnostics]\n")
		fmt.Fprintf(os.Stderr, "input:  %s\n", rep.Input)
		fmt.Fprintf(os.Stderr, "base:   %s\n", rep.BaseDir)
		fmt.Fprintf(os.Stderr, "output: %s\n", rep.Output)
		fmt.Fprintf(os.Stderr, "tree:   files=%d bytes=%d\n", rep.TreeFileCount, rep.TreeByteCount)
		if len(rep.TreeTopDirs) > 0 {
			fmt.Fprintf(os.Stderr, "dirs:   %s\n", strings.Join(rep.TreeTopDirs, ", "))
		}
		if rep.LoadError != "" {
			fmt.Fprintf(os.Stderr, "load:   ERROR %s\n", rep.LoadError)
		}
		if rep.ValidateError != "" {
			fmt.Fprintf(os.Stderr, "valid:  WARN  %s\n", rep.ValidateError)
		}
		fmt.Fprintf(os.Stderr, "refs:   attempted=%d remaining_after_bundle=%d\n", len(rep.RefAttempts), rep.RemainingRefs)

		// Failures
		var fails []refEvent
		for _, a := range rep.RefAttempts {
			if !a.OK {
				fails = append(fails, a)
			}
		}
		if len(fails) > 0 {
			fmt.Fprintf(os.Stderr, "\nfailed refs (%d):\n", len(fails))
			for _, f := range fails {
				fmt.Fprintf(os.Stderr, "  - %s :: %s\n", f.URI, f.Error)
			}
		}

		// Largest refs
		var oks []refEvent
		for _, a := range rep.RefAttempts {
			if a.OK {
				oks = append(oks, a)
			}
		}
		sort.Slice(oks, func(i, j int) bool { return oks[i].Bytes > oks[j].Bytes })
		if len(oks) > 0 {
			fmt.Fprintf(os.Stderr, "\nlargest refs:\n")
			for i := 0; i < len(oks) && i < 8; i++ {
				fmt.Fprintf(os.Stderr, "  - %7dB %s\n", oks[i].Bytes, oks[i].URI)
			}
		}

		if rep.RemainingRefs > 0 {
			fmt.Fprintf(os.Stderr, "\nNOTE: remaining $ref may be internal (#/...) which is normal; failures above indicate missing external refs.\n")
		}
		fmt.Fprintln(os.Stderr)
	}

	if jsonPath != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		_ = os.MkdirAll(filepath.Dir(jsonPath), 0o755)
		_ = os.WriteFile(jsonPath, append(b, '\n'), 0o644)
	}
}

func treeStats(root string, topDepth int) (files int, bytes int64, topDirs []string) {
	seen := map[string]bool{}
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		if info.IsDir() {
			rel, _ := filepath.Rel(root, p)
			if rel == "." {
				return nil
			}
			parts := strings.Split(rel, string(filepath.Separator))
			if len(parts) >= 1 && len(parts) <= topDepth {
				seen[parts[0]] = true
			}
			return nil
		}
		files++
		bytes += info.Size()
		return nil
	})
	for d := range seen {
		topDirs = append(topDirs, d)
	}
	sort.Strings(topDirs)
	return
}

// Counts "$ref" occurrences in marshaled JSON (quick signal).
func countRefsInJSON(j []byte) int {
	return bytes.Count(j, []byte(`"$ref"`))
}

func loadURIWithCap(u *url.URL, baseDir string, maxBytes int) ([]byte, error) {
	switch u.Scheme {
	case "", "file":
		// Prefer URL path when present (file://...).
		path := u.Path

		// If Path is empty, kin-openapi may be handing us a "URI" which is actually a filesystem path.
		// In that case, use it as-is (do NOT join baseDir again).
		if path == "" {
			path = u.String()
		}

		// Normalize ./ prefix and slashes
		path = strings.TrimPrefix(path, "file://")
		path = filepath.FromSlash(path)

		// If path is relative, resolve against baseDir *only if* it isn't already rooted under baseDir.
		if !filepath.IsAbs(path) {
			// If it already begins with baseDir (common when caller passes a repo-relative path),
			// don't double-prepend.
			cleanedBase := filepath.Clean(baseDir)
			cleanedPath := filepath.Clean(path)

			if !strings.HasPrefix(cleanedPath+string(filepath.Separator), cleanedBase+string(filepath.Separator)) &&
				cleanedPath != cleanedBase {
				path = filepath.Join(baseDir, path)
			} else {
				path = cleanedPath
			}
		}

		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", path, err)
		}
		defer f.Close()
		return readAllCap(f, maxBytes)

	case "http", "https":
		req, _ := httpNewRequest(u.String())
		resp, err := httpDo(req)
		if err != nil {
			return nil, fmt.Errorf("http get %s: %w", u.String(), err)
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return nil, fmt.Errorf("http %d %s: %s", resp.StatusCode, u.String(), strings.TrimSpace(string(b)))
		}
		return readAllCap(resp.Body, maxBytes)

	default:
		return nil, fmt.Errorf("unsupported scheme %q for %s", u.Scheme, u.String())
	}
}

func readAllCap(r io.Reader, max int) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > max {
		return nil, fmt.Errorf("ref too large: %d bytes (max %d)", len(b), max)
	}
	return b, nil
}

func httpNewRequest(u string) (*http.Request, error) {
	return http.NewRequest("GET", u, nil)
}

func httpDo(req *http.Request) (*http.Response, error) {
	return http.DefaultClient.Do(req)
}
