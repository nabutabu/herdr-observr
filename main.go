package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nabutabu/herdr-observr/internal/app"
	"github.com/nabutabu/herdr-observr/internal/version"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "version", "--version", "-v", "-V":
			fmt.Println("herdr-observr", version.Version)
			return
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := app.New().Run(ctx); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
