package server

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/calypr/syfon/internal/config"
)

const routeOrderFixture = "testdata/route_order.tsv"

type inventoryRoute struct {
	Method string
	Path   string
	Name   string
	Owner  string
}

func TestMountedRouteParity(t *testing.T) {
	expected := readRouteOrder(t)
	app := buildMockServerRouterWithRoutes(config.RoutesConfig{
		Docs: true, Ga4gh: true, Metrics: true, Internal: true, LFS: true,
	})
	actual := app.GetRoutes(false)

	if len(actual) != len(expected) {
		t.Errorf("mounted route count = %d, inventory count = %d", len(actual), len(expected))
	}
	for i, want := range expected[:min(len(actual), len(expected))] {
		got := actual[i]
		if got.Method != want.Method || got.Path != want.Path || got.Name != want.Name {
			t.Errorf("route %d = %s %s name %q, want %s %s name %q", i, got.Method, got.Path, got.Name, want.Method, want.Path, want.Name)
		}
	}
	for i := len(expected); i < len(actual); i++ {
		got := actual[i]
		t.Errorf("unexpected route %d = %s %s name %q", i, got.Method, got.Path, got.Name)
	}
}

func readRouteOrder(t *testing.T) []inventoryRoute {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed while locating route inventory")
	}
	file, err := os.Open(filepath.Join(filepath.Dir(source), routeOrderFixture))
	if err != nil {
		t.Fatalf("open route inventory: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = '\t'
	reader.FieldsPerRecord = 4
	header, err := reader.Read()
	if err != nil {
		t.Fatalf("read route inventory header: %v", err)
	}
	if fmt.Sprint(header) != "[method path name owner]" {
		t.Fatalf("unexpected route inventory header: %v", header)
	}

	var routes []inventoryRoute
	for {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("read route inventory: %v", err)
		}
		if row[0] == "" || row[1] == "" || row[3] == "" {
			t.Fatalf("invalid route inventory row: %v", row)
		}
		routes = append(routes, inventoryRoute{Method: row[0], Path: row[1], Name: row[2], Owner: row[3]})
	}
	return routes
}
