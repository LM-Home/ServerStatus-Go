package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ServiceStatus/pkg/collector"
	"ServiceStatus/pkg/common"
	"ServiceStatus/pkg/config"
	"ServiceStatus/pkg/monitor"
	"ServiceStatus/pkg/sender"
)

func main() {
	// 1. Load config
	cfg := config.LoadConfig()

	// 1.5 Setup Logger
	level := slog.LevelInfo
	if cfg.Debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	// 2. Initialize store
	store := common.NewStore()

	// 3. Initialize components
	coll := collector.NewCollector(cfg, store)
	mon := monitor.NewMonitor(cfg, store)
	send := sender.NewSender(cfg, store, mon)

	// 4. Setup context and signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 5. Start background tasks
	coll.Start()
	mon.Start(ctx)

	// Periodic collection
	go func() {
		ticker := time.NewTicker(time.Duration(cfg.Interval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				coll.CollectAll()
			}
		}
	}()

	// 6. Start sender (main loop)
	go send.Start(ctx)

	slog.Info("ServerStatus-Go client started")

	// Wait for signal
	<-sigCh
	slog.Info("Shutting down...")
	cancel()
	time.Sleep(1 * time.Second)
}
