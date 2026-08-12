package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
	// a cursor is useless once the entries it points at have aged out, so it
	// expires on the same clock. Nothing sweeps it — a dead pod's cursor just
	// expires on its own.
	cursorTTL = refreshLogTTL
	// finite so ctx cancellation is noticed promptly on shutdown
	refreshLogBlock = 5 * time.Second
	// a refresh the app keeps rejecting must not wedge every later clear for
	// this pod, so retries are bounded before the cursor moves past it
	refreshEntryMaxRounds = 10
	refreshEntryRetryWait = 5 * time.Second
)

func refreshLogKey(serviceName string) string {
	return fmt.Sprintf("inmem:refreshlog:%s", serviceName)
}

func (s *Sidecar) cursorKey() string {
	return fmt.Sprintf("inmem:cursor:%s:%s", s.AppInfo.ServiceName, s.AppInfo.PodName)
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

// runPubSubLoop handles targeted pod requests only. Refreshes come off the
// durable log instead — see ConsumeRefreshLog.
func (s *Sidecar) runPubSubLoop(ctx context.Context) error {
	requestCh := podRequestChannel(s.AppInfo.ServiceName, s.AppInfo.PodName)

	sub := s.Redis.Subscribe(ctx, requestCh)
	defer sub.Close()

	// confirm subscription is active before proceeding
	if _, err := sub.Receive(ctx); err != nil {
		return fmt.Errorf("subscribe confirm: %w", err)
	}

	ch := sub.Channel()
	log.Printf("subscribed to [%s]", requestCh)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return fmt.Errorf("channel closed")
			}
			s.handlePodRequest(ctx, msg.Payload)
		}
	}
}

// applyRefreshWithRetry clears the local app's cache, retrying a few times.
// Reports whether the clear actually landed.
func (s *Sidecar) applyRefreshWithRetry(ctx context.Context, m PubSubMessage) bool {
	var payload struct {
		KeyInfix *string `json:"keyInfix"`
	}
	if err := json.Unmarshal(m.Payload, &payload); err != nil {
		log.Printf("refresh: bad payload, treating as full clear: %v", err)
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
			log.Printf("refresh attempt %d/%d failed: %v", attempt, maxBroadcastRetries, err)
			select {
			case <-ctx.Done():
				return false
			case <-time.After(time.Duration(attempt) * time.Second):
			}
			continue
		}
		drainClose(resp)
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("app returned %d", resp.StatusCode)
			log.Printf("refresh attempt %d/%d: %v", attempt, maxBroadcastRetries, lastErr)
			select {
			case <-ctx.Done():
				return false
			case <-time.After(time.Duration(attempt) * time.Second):
			}
			continue
		}
		log.Printf("refresh from %s applied", m.OriginPod)
		applied = true
		s.sendRefreshAck(ctx, m.ReplyTo, true, "")
		return true
	}
	log.Printf("refresh from %s failed after %d attempts: %v", m.OriginPod, maxBroadcastRetries, lastErr)
	s.sendRefreshAck(ctx, m.ReplyTo, false, lastErr.Error())
	return false
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

// publishRefresh appends a refresh to a service's durable log, which is how
// every pod of that service receives it. Failing to append means nobody clears,
// so unlike a dropped pub/sub message this is a hard error.
func (s *Sidecar) publishRefresh(ctx context.Context, serviceName string, appPayload []byte, replyTo string) error {
	streamID, err := s.appendRefreshLog(ctx, serviceName, appPayload, replyTo)
	if err != nil {
		return fmt.Errorf("refresh log append: %w", err)
	}

	// Compatibility shim: sidecars from before the log consumed refreshes over
	// pub/sub. Publishing keeps them clearing during a rolling upgrade. Safe to
	// drop once every sidecar reads the log.
	msg, _ := json.Marshal(PubSubMessage{
		Action:    "refresh",
		Payload:   appPayload,
		OriginPod: s.AppInfo.PodName,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		ReplyTo:   replyTo,
		StreamID:  streamID,
	})
	if err := s.Redis.Publish(ctx, pubsubChannel(serviceName), string(msg)).Err(); err != nil {
		log.Printf("legacy refresh publish failed (log delivery unaffected): %v", err)
	}
	return nil
}

func (s *Sidecar) appendRefreshLog(ctx context.Context, serviceName string, appPayload []byte, replyTo string) (string, error) {
	key := refreshLogKey(serviceName)
	id, err := s.Redis.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		MaxLen: refreshLogMaxLen,
		Approx: true,
		Values: map[string]any{
			"payload":   string(appPayload),
			"originPod": s.AppInfo.PodName,
			"replyTo":   replyTo,
		},
	}).Result()
	if err != nil {
		return "", err
	}
	// self-cleaning, like every other key shudhi writes
	s.Redis.Expire(ctx, key, refreshLogTTL)
	return id, nil
}

// ConsumeRefreshLog is how this pod receives cache clears. It blocks on the
// service's refresh log and applies each entry in order, committing its cursor
// to Redis only after the app has actually cleared.
//
// The cursor is per-pod and durable, so a sidecar that drops its connection —
// or restarts entirely — resumes exactly where it left off instead of missing
// whatever landed in the gap. Deliberately not a consumer group: a group
// distributes each entry to one member, which would leave every other pod
// serving stale cache.
func (s *Sidecar) ConsumeRefreshLog(ctx context.Context) {
	key := refreshLogKey(s.AppInfo.ServiceName)

	cursor, err := s.loadCursor(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("refresh log: resolving cursor failed: %v (retrying in 3s)", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
		cursor = "0-0"
	}
	log.Printf("consuming %s from %s", key, cursor)

	for {
		if ctx.Err() != nil {
			return
		}

		streams, err := s.Redis.XRead(ctx, &redis.XReadArgs{
			Streams: []string{key, cursor},
			Count:   refreshLogMaxLen,
			Block:   refreshLogBlock,
		}).Result()
		if err == redis.Nil {
			continue // block expired with nothing new
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("refresh log: read from %s failed: %v (retrying in 3s)", cursor, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
			continue
		}

		for _, stream := range streams {
			for _, entry := range stream.Messages {
				if ctx.Err() != nil {
					return
				}
				s.applyRefreshEntry(ctx, entry)
				cursor = entry.ID
				s.saveCursor(ctx, cursor)
			}
		}
	}
}

// applyRefreshEntry clears the local app for one log entry, retrying until it
// lands. It returns once applied, or once retries are exhausted — the caller
// commits the cursor either way, since a refresh the app keeps rejecting must
// not wedge every clear behind it.
func (s *Sidecar) applyRefreshEntry(ctx context.Context, entry redis.XMessage) {
	origin, _ := entry.Values["originPod"].(string)
	if origin == s.AppInfo.PodName {
		return // the handler that published this already cleared us locally
	}
	payload, _ := entry.Values["payload"].(string)
	replyTo, _ := entry.Values["replyTo"].(string)

	m := PubSubMessage{
		Action:    "refresh",
		Payload:   json.RawMessage(payload),
		OriginPod: origin,
		ReplyTo:   replyTo,
	}

	for round := 1; round <= refreshEntryMaxRounds; round++ {
		if s.applyRefreshWithRetry(ctx, m) {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if round < refreshEntryMaxRounds {
			log.Printf("refresh %s not applied, retrying (round %d/%d)", entry.ID, round, refreshEntryMaxRounds)
			// the caller is long past its ack timeout; only the first round acks
			m.ReplyTo = ""
			select {
			case <-ctx.Done():
				return
			case <-time.After(refreshEntryRetryWait):
			}
		}
	}
	log.Printf("ERROR: giving up on refresh %s from %s after %d rounds — this pod may be serving stale cache",
		entry.ID, origin, refreshEntryMaxRounds)
}

// loadCursor returns where this pod should resume reading. A stored cursor wins;
// otherwise it starts at the tail, since anything already logged predates this
// pod's cache and replaying it would be busywork.
func (s *Sidecar) loadCursor(ctx context.Context) (string, error) {
	id, err := s.Redis.Get(ctx, s.cursorKey()).Result()
	if err == nil && id != "" {
		return id, nil
	}
	if err != nil && err != redis.Nil {
		return "", err
	}

	tail, err := s.Redis.XRevRangeN(ctx, refreshLogKey(s.AppInfo.ServiceName), "+", "-", 1).Result()
	if err != nil {
		return "", err
	}
	if len(tail) > 0 {
		return tail[0].ID, nil
	}
	return "0-0", nil
}

func (s *Sidecar) saveCursor(ctx context.Context, id string) {
	// detached: a cancelled ctx must not lose a cursor for work already done,
	// or the pod would re-apply the same clears on its next start
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.Redis.Set(saveCtx, s.cursorKey(), id, cursorTTL).Err(); err != nil {
		log.Printf("refresh log: saving cursor %s failed: %v", id, err)
	}
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
