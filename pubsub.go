package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

)

type PubSubMessage struct {
	Action    string          `json:"action"`
	Payload   json.RawMessage `json:"payload"`
	OriginPod string          `json:"originPod"`
	Timestamp string          `json:"timestamp"`
}

// Subscribe to two channels:
// 1. inmem:<serviceName>        — broadcast ops (refresh) from any sidecar
// 2. inmem:req:<svc>:<podName>  — targeted requests (get) for this pod
func (s *Sidecar) SubscribePubSub(ctx context.Context) {
	broadcastCh := pubsubChannel(s.AppInfo.ServiceName)
	requestCh := podRequestChannel(s.AppInfo.ServiceName, s.AppInfo.PodName)

	sub := s.Redis.Subscribe(ctx, broadcastCh, requestCh)
	ch := sub.Channel()

	log.Printf("subscribed to [%s, %s]", broadcastCh, requestCh)
	for {
		select {
		case <-ctx.Done():
			sub.Close()
			return
		case msg := <-ch:
			if msg == nil {
				return
			}
			switch msg.Channel {
			case broadcastCh:
				s.handleBroadcast(msg.Payload)
			case requestCh:
				s.handlePodRequest(ctx, msg.Payload)
			}
		}
	}
}

func (s *Sidecar) handleBroadcast(payload string) {
	var m PubSubMessage
	if err := json.Unmarshal([]byte(payload), &m); err != nil {
		log.Printf("pubsub: bad message: %v", err)
		return
	}
	if m.OriginPod == s.AppInfo.PodName {
		return
	}
	switch m.Action {
	case "refresh":
		resp, err := s.HTTP.Post(
			s.Config.AppURL+"/internal/inMem/refresh",
			"application/json",
			strings.NewReader(string(m.Payload)),
		)
		if err != nil {
			log.Printf("pubsub refresh failed: %v", err)
			return
		}
		resp.Body.Close()
		log.Printf("refresh from %s applied", m.OriginPod)
	}
}

// PodRequest is a targeted request sent to a specific pod via pub/sub
type PodRequest struct {
	Action  string          `json:"action"`
	Payload json.RawMessage `json:"payload"`
	ReplyTo string          `json:"replyTo"`
}

func (s *Sidecar) handlePodRequest(ctx context.Context, payload string) {
	var req PodRequest
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		log.Printf("pod request: bad message: %v", err)
		return
	}
	switch req.Action {
	case "get":
		resp, err := s.HTTP.Post(
			s.Config.AppURL+"/internal/inMem/get",
			"application/json",
			strings.NewReader(string(req.Payload)),
		)
		if err != nil {
			s.Redis.Publish(ctx, req.ReplyTo, `{"error":"app unreachable"}`)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		s.Redis.Publish(ctx, req.ReplyTo, string(body))
	}
}

// pubsubGet sends a get request to a specific pod via pub/sub and waits for response
func (s *Sidecar) pubsubGet(ctx context.Context, serviceName, podName, key string) ([]byte, error) {
	replyTo := fmt.Sprintf("inmem:reply:%s:%d", s.AppInfo.PodName, time.Now().UnixNano())

	// subscribe to reply channel before publishing
	sub := s.Redis.Subscribe(ctx, replyTo)
	defer sub.Close()

	payload, _ := json.Marshal(map[string]string{"key": key})
	req, _ := json.Marshal(PodRequest{
		Action:  "get",
		Payload: payload,
		ReplyTo: replyTo,
	})

	targetCh := podRequestChannel(serviceName, podName)
	if err := s.Redis.Publish(ctx, targetCh, string(req)).Err(); err != nil {
		return nil, fmt.Errorf("publish failed: %w", err)
	}

	// wait for response with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg, err := sub.ReceiveMessage(timeoutCtx)
	if err != nil {
		return nil, fmt.Errorf("timeout waiting for response: %w", err)
	}
	return []byte(msg.Payload), nil
}

// publishRefresh publishes a refresh to any service's broadcast channel
func (s *Sidecar) publishRefresh(ctx context.Context, serviceName string, appPayload []byte) error {
	msg, _ := json.Marshal(PubSubMessage{
		Action:    "refresh",
		Payload:   appPayload,
		OriginPod: s.AppInfo.PodName,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
	return s.Redis.Publish(ctx, pubsubChannel(serviceName), string(msg)).Err()
}
