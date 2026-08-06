package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

const (
	podTTL       = 60 * time.Second
	heartbeatInt = 30 * time.Second
)

func (s *sidecar) Register(ctx context.Context) error {
	return s.Redis.Set(ctx, s.podKey(), s.sidecarURL(), podTTL).Err()
}

func (s *sidecar) Deregister(ctx context.Context) {
	if err := s.Redis.Del(ctx, s.podKey()).Err(); err != nil {
		log.Printf("deregister failed: %v", err)
	}
	// clean up registered keys
	s.Redis.Del(ctx, s.keysKey())
	log.Println("deregistered from redis")
}

// Heartbeat re-registers the pod in Redis on each tick.
// Returns an error if too many consecutive failures occur (triggers reconnect).
func (s *sidecar) Heartbeat(ctx context.Context) error {
	ticker := time.NewTicker(heartbeatInt)
	defer ticker.Stop()
	consecutiveFails := 0
	const maxFails = 3

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Register(ctx); err != nil {
				consecutiveFails++
				log.Printf("heartbeat failed (%d/%d): %v", consecutiveFails, maxFails, err)
				if consecutiveFails >= maxFails {
					return fmt.Errorf("heartbeat: %d consecutive failures", consecutiveFails)
				}
			} else {
				if consecutiveFails > 0 {
					log.Printf("heartbeat recovered after %d failures", consecutiveFails)
				}
				consecutiveFails = 0
			}
		}
	}
}
