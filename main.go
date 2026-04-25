package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
)

func main() {
	cfg := LoadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s := NewSidecar(cfg)

	// start HTTP server immediately — health + registerKey always work
	srv := &http.Server{Addr: ":" + cfg.SidecarPort, Handler: s.Routes()}
	go func() {
		log.Printf("sidecar listening on :%s", cfg.SidecarPort)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("http server error: %v", err)
		}
	}()

	// connect to app in background — retries until success or shutdown
	go s.WaitForApp(ctx)

	<-ctx.Done()
	log.Println("shutting down...")
	if s.IsReady() {
		s.Deregister(context.Background())
	}
	srv.Close()
}
