// Command server is the Queuemaxxing HTTP API process: it parses
// configuration, opens the durable queue, and serves the HTTP API —
// including the embedded web console at GET / — until SIGINT or SIGTERM.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"queuemaxxing/pkg/api"
	"queuemaxxing/pkg/model"
	"queuemaxxing/pkg/service"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := parseConfig()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(cfg.dataPath), 0o755); err != nil {
		return fmt.Errorf("creating data directory: %w", err)
	}

	svc, err := service.NewQueueService(cfg.dataPath, cfg.mode)
	if err != nil {
		return fmt.Errorf("opening queue: %w", err)
	}

	srv := &http.Server{
		Addr:              net.JoinHostPort("", cfg.port),
		Handler:           api.NewHandler(svc),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	listenErr := make(chan error, 1)
	go func() {
		log.Printf("listening on %s (mode=%s data=%s)", srv.Addr, cfg.mode, cfg.dataPath)
		listenErr <- srv.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-listenErr:
		closeErr := svc.Close()
		if errors.Is(err, http.ErrServerClosed) {
			return closeErr
		}
		return errors.Join(fmt.Errorf("http server: %w", err), closeErr)
	case <-ctx.Done():
		log.Printf("shutdown signal received, closing")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		httpErr := srv.Shutdown(shutdownCtx)
		closeErr := svc.Close()
		return errors.Join(httpErr, closeErr)
	}
}

type config struct {
	port     string
	mode     model.QueueMode
	dataPath string
}

func parseConfig() (config, error) {
	port := flag.String("port", getenv("PORT", "8080"), "HTTP listen port (env: PORT)")
	modeStr := flag.String("mode", getenv("QUEUE_MODE", string(model.ModeFIFO)), "queue ordering mode: FIFO or LIFO (env: QUEUE_MODE)")
	dataPath := flag.String("data", getenv("DATA_PATH", "./data/queue.wal"), "WAL file path (env: DATA_PATH)")
	flag.Parse()

	portVal := strings.TrimPrefix(strings.TrimSpace(*port), ":")
	if portVal == "" {
		return config{}, fmt.Errorf("port must not be empty")
	}

	mode, err := model.ParseQueueMode(strings.ToUpper(strings.TrimSpace(*modeStr)))
	if err != nil {
		return config{}, err
	}

	return config{
		port:     portVal,
		mode:     mode,
		dataPath: *dataPath,
	}, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
