// internal/apigen/main.go
package main

import (
	"log"
	"os"
)

const defaultSpecPath = "openapi/openapi.yaml"

func main() {
	specPath := os.Getenv("OPENAPI_SPEC")
	if specPath == "" {
		specPath = defaultSpecPath
	}

	validator, err := newSpecValidator(specPath)
	if err != nil {
		log.Fatalf("openapi validator: %v", err)
	}

	routes := sw.ApiHandleFunctions{}
	router := sw.NewRouter(routes)
	router.Use(validator)

	log.Printf("Server started")
	log.Fatal(router.Run(":8080"))
}
