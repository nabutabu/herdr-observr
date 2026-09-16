package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nabutabu/herdr-observr/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := app.New().Run(ctx); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
