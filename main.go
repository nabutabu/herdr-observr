package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nabutabu/herdr-observr/internal/app"
	"github.com/nabutabu/herdr-observr/internal/config"
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

	// (4.2) Load the plugin's .env from HERDR_PLUGIN_CONFIG_DIR. A load error
	// must never kill the daemon: warn and continue with env-only defaults.
	cfg, err := config.Load()
	if err != nil {
		slog.Warn("plugin config load failed; using env defaults", "error", err)
	}

	if err := app.New(cfg).Run(ctx); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
