package server

import (
	"fmt"
	"log"
	"net/http"

	openapi "github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db"
	"github.com/calypr/drs-server/service"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "serve",
	Short: "Starts the DRS Object API server",
	Run: func(cmd *cobra.Command, args []string) {
		// Init DB
		database := db.NewInMemoryDB()

		// Init Service
		service := service.NewObjectsAPIService(database)

		// Init Controller
		controller := openapi.NewObjectsAPIController(service)

		// Init Router
		router := openapi.NewRouter(controller)

		addr := ":8080"
		fmt.Printf("Server starting on %s\n", addr)
		log.Fatal(http.ListenAndServe(addr, router))
	},
}
