package main

import (
	"context"
	"log"
	"time"
)

const (
	podTTL       = 60 * time.Second
	heartbeatInt = 30 * time.Second
)

func (s *Sidecar) Register(ctx context.Context) error {
	return s.Redis.Set(ctx, s.podKey(), s.sidecarURL(), podTTL).Err()
}

func (s *Sidecar) Deregister(ctx context.Context) {
	if err := s.Redis.Del(ctx, s.podKey()).Err(); err != nil {
		log.Printf("deregister failed: %v", err)
	}
	// clean up registered keys
	s.Redis.Del(ctx, s.keysKey())
	log.Println("deregistered from redis")
}

func (s *Sidecar) Heartbeat(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInt)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Register(ctx); err != nil {
				log.Printf("heartbeat failed: %v", err)
			}
		}
	}
}
