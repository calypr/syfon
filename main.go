package main

import (
	"log/slog"
	"os"

	"github.com/calypr/syfon/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		slog.Error("command execution failed", "err", err)
		os.Exit(1)
	}
}
