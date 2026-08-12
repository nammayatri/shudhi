package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type PubSubMessage struct {
	Action    string          `json:"action"`
	Payload   json.RawMessage `json:"payload"`
	OriginPod string          `json:"originPod"`
	Timestamp string          `json:"timestamp"`
	ReplyTo   string          `json:"replyTo,omitempty"`
	StreamID  string          `json:"streamId,omitempty"` // position in the durable refresh log
}

const (
	// the refresh log only has to bridge a disconnect, not keep history
	refreshLogMaxLen = 1000
	refreshLogTTL    = 24 * time.Hour
	// bounds how long a missed refresh can go unnoticed when go-redis
	// reconnects the subscription without surfacing an error
	refreshCatchUpInterval = 30 * time.Second
)

func refreshLogKey(serviceName string) string {
	return fmt.Sprintf("inmem:refreshlog:%s", serviceName)
}

type RefreshAck struct {
	PodName string `json:"podName"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// SubscribePubSub subscribes to broadcast and targeted channels.
// It automatically reconnects on failure until ctx is cancelled.
func (s *Sidecar) SubscribePubSub(ctx context.Context) {
	for {
		err := s.runPubSubLoop(ctx)
		if ctx.Err() != nil {
			return // shutting down
		}
		log.Printf("pubsub disconnected: %v — reconnecting in 3s", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}

func (s *Sidecar) runPubSubLoop(ctx context.Context) error {
	broadcastCh := pubsubChannel(s.AppInfo.ServiceName)
	requestCh := podRequestChannel(s.AppInfo.ServiceName, s.AppInfo.PodName)

	sub := s.Redis.Subscribe(ctx, broadcastCh, requestCh)
	defer sub.Close()

	// confirm subscription is active before proceeding
	if _, err := sub.Receive(ctx); err != nil {
		return fmt.Errorf("subscribe confirm: %w", err)
	}

	ch := sub.Channel()
	log.Printf("subscribed to [%s, %s]", broadcastCh, requestCh)

	// pick up anything published while we weren't listening
	s.catchUpRefreshes(ctx)

	catchUp := time.NewTicker(refreshCatchUpInterval)
	defer catchUp.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-catchUp.C:
			s.catchUpRefreshes(ctx)
		case msg, ok := <-ch:
			if !ok {
				return fmt.Errorf("channel closed")
			}
			switch msg.Channel {
			case broadcastCh:
				s.handleBroadcast(ctx, msg.Payload)
			case requestCh:
				s.handlePodRequest(ctx, msg.Payload)
			}
		}
	}
}

func (s *Sidecar) handleBroadcast(ctx context.Context, payload string) {
	var m PubSubMessage
	if err := json.Unmarshal([]byte(payload), &m); err != nil {
		log.Printf("pubsub: bad message: %v", err)
		return
	}
	// advance the cursor even for our own messages, so catch-up doesn't
	// replay a refresh this pod already handled locally
	s.setLastRefreshID(m.StreamID)

	if m.OriginPod == s.AppInfo.PodName {
		return
	}
	switch m.Action {
	case "refresh":
		s.applyRefreshWithRetry(ctx, m)
	}
}

func (s *Sidecar) applyRefreshWithRetry(ctx context.Context, m PubSubMessage) {
	var payload struct {
		KeyInfix *string `json:"keyInfix"`
	}
	if err := json.Unmarshal(m.Payload, &payload); err != nil {
		log.Printf("pubsub refresh: bad payload, treating as full clear: %v", err)
	}

	// registry entries come out first and go back on any exit that isn't a
	// successful clear — including the ctx.Done() bailouts inside the loop
	snap := s.deregisterKeys(ctx, payload.KeyInfix)
	applied := false
	defer func() {
		if !applied {
			s.restoreKeys(ctx, snap)
		}
	}()

	var lastErr error
	for attempt := 1; attempt <= maxBroadcastRetries; attempt++ {
		resp, err := s.doPost(
			ctx,
			s.Config.AppURL+"/internal/inMem/refresh",
			"application/json",
			strings.NewReader(string(m.Payload)),
		)
		if err != nil {
			lastErr = err
			log.Printf("pubsub refresh attempt %d/%d failed: %v", attempt, maxBroadcastRetries, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(attempt) * time.Second):
			}
			continue
		}
		drainClose(resp)
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("app returned %d", resp.StatusCode)
			log.Printf("pubsub refresh attempt %d/%d: %v", attempt, maxBroadcastRetries, lastErr)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(attempt) * time.Second):
			}
			continue
		}
		log.Printf("refresh from %s applied", m.OriginPod)
		applied = true
		s.sendRefreshAck(ctx, m.ReplyTo, true, "")
		return
	}
	log.Printf("pubsub refresh from %s failed after %d retries: %v", m.OriginPod, maxBroadcastRetries, lastErr)
	s.sendRefreshAck(ctx, m.ReplyTo, false, lastErr.Error())
}

func (s *Sidecar) sendRefreshAck(ctx context.Context, replyTo string, success bool, errMsg string) {
	if replyTo == "" {
		return
	}
	ack, _ := json.Marshal(RefreshAck{
		PodName: s.AppInfo.PodName,
		Success: success,
		Error:   errMsg,
	})
	s.Redis.Publish(ctx, replyTo, string(ack))
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
		resp, err := s.doPost(
			ctx,
			s.Config.AppURL+"/internal/inMem/get",
			"application/json",
			strings.NewReader(string(req.Payload)),
		)
		if err != nil {
			s.Redis.Publish(ctx, req.ReplyTo, `{"error":"app unreachable"}`)
			return
		}
		defer drainClose(resp)
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

// publishRefresh publishes a refresh to any service's broadcast channel.
// The refresh is appended to the durable log first: pub/sub drops messages for
// disconnected subscribers, so the log is what lets a pod that missed the live
// broadcast catch up when it resubscribes.
func (s *Sidecar) publishRefresh(ctx context.Context, serviceName string, appPayload []byte, replyTo string) error {
	streamID, err := s.appendRefreshLog(ctx, serviceName, appPayload)
	if err != nil {
		// the live broadcast still goes out — reachable pods clear either way
		log.Printf("refresh log append failed, missed-message catch-up unavailable for this refresh: %v", err)
	}

	msg, _ := json.Marshal(PubSubMessage{
		Action:    "refresh",
		Payload:   appPayload,
		OriginPod: s.AppInfo.PodName,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		ReplyTo:   replyTo,
		StreamID:  streamID,
	})
	return s.Redis.Publish(ctx, pubsubChannel(serviceName), string(msg)).Err()
}

func (s *Sidecar) appendRefreshLog(ctx context.Context, serviceName string, appPayload []byte) (string, error) {
	key := refreshLogKey(serviceName)
	id, err := s.Redis.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		MaxLen: refreshLogMaxLen,
		Approx: true,
		Values: map[string]any{
			"payload":   string(appPayload),
			"originPod": s.AppInfo.PodName,
		},
	}).Result()
	if err != nil {
		return "", err
	}
	// self-cleaning, like every other key shudhi writes
	s.Redis.Expire(ctx, key, refreshLogTTL)
	return id, nil
}

// catchUpRefreshes replays refreshes that landed while this pod wasn't
// listening. Redis pub/sub has no replay for a disconnected subscriber, so
// without this a pod that blips during a clear serves stale cache indefinitely.
//
// Runs on every (re)subscribe and on a ticker — go-redis reconnects the
// underlying connection transparently, so a dropped subscription doesn't
// necessarily surface as an error we could hook.
func (s *Sidecar) catchUpRefreshes(ctx context.Context) {
	key := refreshLogKey(s.AppInfo.ServiceName)
	last := s.getLastRefreshID()

	// first time through: start at the tail. Anything already in the log
	// predates this pod's cache, so there's nothing to catch up on.
	if last == "" {
		tail, err := s.Redis.XRevRangeN(ctx, key, "+", "-", 1).Result()
		if err != nil {
			log.Printf("refresh log: reading tail failed: %v", err)
			return
		}
		if len(tail) > 0 {
			s.setLastRefreshID(tail[0].ID)
		} else {
			s.setLastRefreshID("0-0")
		}
		return
	}

	streams, err := s.Redis.XRead(ctx, &redis.XReadArgs{
		Streams: []string{key, last},
		Count:   refreshLogMaxLen,
		Block:   -1, // don't block; we just want whatever is already there
	}).Result()
	if err == redis.Nil {
		return // nothing missed
	}
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("refresh log: read from %s failed: %v", last, err)
		}
		return
	}

	for _, stream := range streams {
		for _, entry := range stream.Messages {
			s.setLastRefreshID(entry.ID)

			origin, _ := entry.Values["originPod"].(string)
			if origin == s.AppInfo.PodName {
				continue
			}
			payload, _ := entry.Values["payload"].(string)

			log.Printf("catch-up: replaying missed refresh %s from %s", entry.ID, origin)
			// no ReplyTo — the original caller stopped waiting for acks long ago
			s.applyRefreshWithRetry(ctx, PubSubMessage{
				Action:    "refresh",
				Payload:   json.RawMessage(payload),
				OriginPod: origin,
			})
		}
	}
}

func (s *Sidecar) setLastRefreshID(id string) {
	if id == "" {
		return
	}
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	// only ever move forward, so a stale or duplicate delivery can't rewind the
	// cursor and cause the same refresh to replay on every reconnect
	if s.lastRefreshID == "" || streamIDLess(s.lastRefreshID, id) {
		s.lastRefreshID = id
	}
}

func (s *Sidecar) getLastRefreshID() string {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	return s.lastRefreshID
}

// streamIDLess reports whether Redis stream ID a sorts before b. IDs are
// "<millis>-<seq>" with no zero padding, so a plain string compare gets
// "9-0" vs "10-0" backwards.
func streamIDLess(a, b string) bool {
	aMS, aSeq := splitStreamID(a)
	bMS, bSeq := splitStreamID(b)
	if aMS != bMS {
		return aMS < bMS
	}
	return aSeq < bSeq
}

func splitStreamID(id string) (ms, seq uint64) {
	base, tail, found := strings.Cut(id, "-")
	ms, _ = strconv.ParseUint(base, 10, 64)
	if found {
		seq, _ = strconv.ParseUint(tail, 10, 64)
	}
	return ms, seq
}

// publishRefreshAndCollectAcks publishes a refresh and waits for per-pod confirmations.
func (s *Sidecar) publishRefreshAndCollectAcks(ctx context.Context, serviceName string, appPayload []byte, expectedPods int) ([]RefreshAck, error) {
	replyTo := fmt.Sprintf("inmem:refresh-ack:%s:%d", s.AppInfo.PodName, time.Now().UnixNano())

	// subscribe before publishing so we don't miss acks
	sub := s.Redis.Subscribe(ctx, replyTo)
	defer sub.Close()

	if err := s.publishRefresh(ctx, serviceName, appPayload, replyTo); err != nil {
		return nil, err
	}

	// the origin pod handles its own refresh locally, so we expect acks from (expectedPods - 1)
	// if this sidecar isn't part of the target service, expect all pods
	expectCount := expectedPods
	if serviceName == s.AppInfo.ServiceName {
		expectCount = expectedPods - 1
	}
	if expectCount <= 0 {
		return nil, nil
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ch := sub.Channel()
	var acks []RefreshAck
	for {
		select {
		case <-timeoutCtx.Done():
			return acks, nil
		case msg, ok := <-ch:
			if !ok {
				return acks, nil
			}
			var ack RefreshAck
			if err := json.Unmarshal([]byte(msg.Payload), &ack); err != nil {
				continue
			}
			acks = append(acks, ack)
			if len(acks) >= expectCount {
				return acks, nil
			}
		}
	}
}
