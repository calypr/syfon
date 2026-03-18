package docs

import (
	"net/http"
	"os"
	"path/filepath"
	"runtime"

	"github.com/gorilla/mux"
)

const swaggerUIHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DRS Server API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = function() {
      window.ui = SwaggerUIBundle({
        url: "/openapi.yaml",
        dom_id: "#swagger-ui"
      });
    };
  </script>
</body>
</html>
`

// RegisterSwaggerRoutes adds Swagger/OpenAPI docs endpoints.
func RegisterSwaggerRoutes(router *mux.Router) {
	router.HandleFunc("/swagger", handleSwaggerUI).Methods(http.MethodGet)
	router.HandleFunc("/swagger/", handleSwaggerUI).Methods(http.MethodGet)
	router.HandleFunc("/openapi.yaml", handleOpenAPISpec).Methods(http.MethodGet)
}

func handleSwaggerUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerUIHTML))
}

func handleOpenAPISpec(w http.ResponseWriter, r *http.Request) {
	specPath, ok := findOpenAPISpecPath()
	if !ok {
		http.Error(w, "OpenAPI spec file not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	http.ServeFile(w, r, specPath)
}

func findOpenAPISpecPath() (string, bool) {
	candidates := []string{
		"apigen/api/openapi.yaml",
		filepath.Join(filepath.Dir(os.Args[0]), "apigen", "api", "openapi.yaml"),
	}

	if _, thisFile, _, ok := runtime.Caller(0); ok {
		repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
		candidates = append(candidates, filepath.Join(repoRoot, "apigen", "api", "openapi.yaml"))
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, true
		}
	}
	return "", false
}
