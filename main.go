package main

import (
	"fmt"
	"os"

	"github.com/calypr/drs-server/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		fmt.Println("Error:", err.Error())
		os.Exit(1)
	}
}
