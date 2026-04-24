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

	s, err := NewSidecar(cfg)
	if err != nil {
		log.Fatalf("init: %v", err)
	}

	// register and start heartbeat
	if err := s.Register(ctx); err != nil {
		log.Fatalf("register: %v", err)
	}
	go s.Heartbeat(ctx)
	go s.SubscribePubSub(ctx)

	srv := &http.Server{Addr: ":" + cfg.SidecarPort, Handler: s.Routes()}

	go func() {
		log.Printf("sidecar listening on :%s", cfg.SidecarPort)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	s.Deregister(context.Background())
	srv.Close()
}
