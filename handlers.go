package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxBroadcastRetries = 3

// --- registerKey: app registers a cached key ---

func (s *sidecar) handleRegisterKey(w http.ResponseWriter, r *http.Request) {
	if !s.IsReady() {
		// accept silently — don't break the app, keys will re-register later
		w.WriteHeader(http.StatusAccepted)
		return
	}

	var req RegisterKeyReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	val, _ := json.Marshal(map[string]any{
		"keySchema":    req.KeySchema,
		"ttlInSeconds": req.TTLInSeconds,
		"registeredAt": time.Now().UTC().Format(time.RFC3339),
	})

	ctx := r.Context()
	key := s.keysKey()
	pipe := s.Redis.Pipeline()
	pipe.HSet(ctx, key, req.KeyName, string(val))
	pipe.Expire(ctx, key, 3*24*time.Hour)
	if _, err := pipe.Exec(ctx); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// --- services: list all service names ---

func (s *sidecar) handleServices(w http.ResponseWriter, r *http.Request) {
	services, err := s.scanServices(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"services": services})
}

func (s *sidecar) scanServices(ctx context.Context) ([]string, error) {
	seen := map[string]bool{}
	var cursor uint64

	for {
		keys, next, err := s.Redis.Scan(ctx, cursor, podScanPattern, 100).Result()
		if err != nil {
			return nil, err
		}

		for _, k := range keys {
			// inmem:pod:<serviceName>:<podName>
			parts := strings.SplitN(k, ":", 4)
			if len(parts) >= 3 {
				seen[parts[2]] = true
			}
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}

	out := make([]string, 0, len(seen))
	for svc := range seen {
		out = append(out, svc)
	}

	return out, nil
}

// --- pods: list live pods for a service ---

func (s *sidecar) handlePods(w http.ResponseWriter, r *http.Request) {
	svc := r.URL.Query().Get("service")
	if svc == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("service param required"))
		return
	}

	pods, err := s.scanPods(r.Context(), svc)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"pods": pods})
}

func (s *sidecar) scanPods(ctx context.Context, svc string) ([]PodInfo, error) {
	var pods []PodInfo
	var cursor uint64
	prefix := podPrefixFor(svc)

	for {
		keys, next, err := s.Redis.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			podName := strings.TrimPrefix(k, prefix)
			url, err := s.Redis.Get(ctx, k).Result()
			if err != nil {
				continue
			}
			pods = append(pods, PodInfo{PodName: podName, SidecarURL: url})
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}

	return pods, nil
}

// --- keys: list registered keys ---

func (s *sidecar) handleKeys(w http.ResponseWriter, r *http.Request) {
	svc := r.URL.Query().Get("service")
	if svc == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("service param required"))
		return
	}
	pod := r.URL.Query().Get("pod")
	pattern := r.URL.Query().Get("pattern")
	limit := r.URL.Query().Get("limit")
	offset := r.URL.Query().Get("offset")

	ctx := r.Context()

	if pod != "" {
		// Live query: proxy to the pod's app /inMem/keys endpoint
		s.proxyKeysToApp(ctx, w, r, svc, pod, pattern, limit, offset)
		return
	}

	// No pod selected: aggregate from Redis hashes (registered keys)
	var result []map[string]any
	pods, err := s.scanPods(ctx, svc)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	for _, p := range pods {
		entries, err := s.getKeysForPod(ctx, svc, p.PodName)
		if err != nil {
			continue
		}

		result = append(result, entries...)
	}

	// Client-side filter for pattern when aggregating from Redis
	if pattern != "" {
		var filtered []map[string]any
		lp := strings.ToLower(pattern)
		for _, entry := range result {
			if name, ok := entry["keyName"].(string); ok && strings.Contains(strings.ToLower(name), lp) {
				filtered = append(filtered, entry)
			}
		}
		result = filtered
	}

	writeJSON(w, http.StatusOK, map[string]any{"keys": result, "source": "registry"})
}

func (s *sidecar) proxyKeysToApp(ctx context.Context, w http.ResponseWriter, r *http.Request, svc, pod, pattern, limit, offset string) {
	// Build /inMem/keys query string for the app
	q := url.Values{}
	if pattern != "" {
		q.Set("pattern", pattern)
	}
	if limit != "" {
		q.Set("limit", limit)
	}
	if offset != "" {
		q.Set("offset", offset)
	}
	targetPath := "/inMem/keys"
	if len(q) > 0 {
		targetPath += "?" + q.Encode()
	}

	// If it's this pod, call local app directly
	if s.isLocalPod(svc, pod) {
		resp, err := s.doGet(ctx, s.Config.AppURL+targetPath)
		if err != nil {
			writeError(w, http.StatusBadGateway, fmt.Errorf("app unreachable: %w", err))
			return
		}
		defer drainClose(resp)
		copyJSON(w, resp)
		return
	}

	// Prevent proxy loops
	if isProxied(r) {
		writeError(w, http.StatusLoopDetected, fmt.Errorf("proxy loop detected"))
		return
	}

	// Proxy to target pod's sidecar
	targetURL, err := s.Redis.Get(ctx, podKeyFor(svc, pod)).Result()
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Errorf("pod not found"))
		return
	}

	// Forward full /api/keys request to target sidecar (which will call its local app)
	fwdQ := r.URL.Query()
	fwdURL := targetURL + "/api/keys?" + fwdQ.Encode()
	proxyReq, err := http.NewRequestWithContext(ctx, http.MethodGet, fwdURL, nil)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Errorf("build proxy request: %w", err))
		return
	}
	proxyReq.Header.Set("X-Shudhi-Proxied", "true")
	if s.Config.InMemToken != "" {
		proxyReq.Header.Set("x-inmem-token", s.Config.InMemToken)
	}

	resp, err := s.ProxyHTTP.Do(proxyReq)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Errorf("sidecar unreachable: %w", err))
		return
	}

	defer drainClose(resp)

	copyJSON(w, resp)
}

func (s *sidecar) getKeysForPod(ctx context.Context, svc, pod string) ([]map[string]any, error) {
	hashKey := keysHashKeyFor(svc, pod)
	entries, err := s.Redis.HGetAll(ctx, hashKey).Result()
	if err != nil {
		return nil, err
	}

	var out []map[string]any
	for keyName, meta := range entries {
		var m map[string]any
		if err := json.Unmarshal([]byte(meta), &m); err != nil {
			continue
		}
		m["keyName"] = keyName
		m["podName"] = pod
		out = append(out, m)
	}

	return out, nil
}

// --- pod/get: query a specific pod for a key's value ---

func (s *sidecar) handlePodGet(w http.ResponseWriter, r *http.Request) {
	var req PodGetReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	ctx := r.Context()

	// if it's this pod, call local app directly
	if s.isLocalPod(req.ServiceName, req.PodName) {
		s.proxyGetToApp(ctx, w, req.Key)
		return
	}

	// prevent infinite proxy loops
	if isProxied(r) {
		writeError(w, http.StatusLoopDetected, fmt.Errorf("proxy loop detected"))
		return
	}

	// try direct HTTP to target sidecar
	targetURL, err := s.Redis.Get(ctx, podKeyFor(req.ServiceName, req.PodName)).Result()
	if err == nil {
		body, _ := json.Marshal(PodGetReq{ServiceName: req.ServiceName, PodName: req.PodName, Key: req.Key})
		proxyReq, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, targetURL+"/api/pod/get", strings.NewReader(string(body)))
		if reqErr != nil {
			log.Printf("build direct request to %s failed, falling back to pubsub: %v", req.PodName, reqErr)
		} else {
			proxyReq.Header.Set("Content-Type", "application/json")
			proxyReq.Header.Set("X-Shudhi-Proxied", "true")
			if s.Config.InMemToken != "" {
				proxyReq.Header.Set("x-inmem-token", s.Config.InMemToken)
			}

			resp, httpErr := s.ProxyHTTP.Do(proxyReq)
			if httpErr == nil {
				defer drainClose(resp)
				copyJSON(w, resp)
				return
			}

			log.Printf("direct HTTP to %s failed, falling back to pubsub: %v", req.PodName, httpErr)
		}
	}

	// fallback: pub/sub RPC
	result, err := s.pubsubGet(ctx, req.ServiceName, req.PodName, req.Key)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Errorf("pod unreachable: %w", err))
		return
	}

	writeRawJSON(w, http.StatusOK, result)
}

func (s *sidecar) proxyGetToApp(ctx context.Context, w http.ResponseWriter, key string) {
	body, _ := json.Marshal(map[string]string{"key": key})
	resp, err := s.doPost(ctx, s.Config.AppURL+"/internal/inMem/get", "application/json", strings.NewReader(string(body)))
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Errorf("app unreachable: %w", err))
		return
	}

	defer drainClose(resp)
	copyJSON(w, resp)
}

// --- refresh: publish to target service's broadcast channel ---

func (s *sidecar) handleRefresh(w http.ResponseWriter, r *http.Request) {
	var req RefreshReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ServiceName == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("serviceName required"))
		return
	}

	ctx := r.Context()
	appPayload, _ := json.Marshal(map[string]any{"keyInfix": req.KeyInfix})

	// get pod count for the target service so we know how many acks to expect
	pods, _ := s.scanPods(ctx, req.ServiceName)
	totalPods := len(pods)

	// build per-pod results
	var results []PodAckResult

	// if this sidecar is part of the target service, refresh locally first
	if req.ServiceName == s.AppInfo.ServiceName {
		resp, err := s.doPost(ctx, s.Config.AppURL+"/internal/inMem/refresh", "application/json",
			strings.NewReader(string(appPayload)))
		if err != nil {
			log.Printf("local refresh failed (will still broadcast): %v", err)
			results = append(results, PodAckResult{PodName: s.AppInfo.PodName, Success: false, Error: err.Error()})
		} else {
			drainClose(resp)
			results = append(results, PodAckResult{PodName: s.AppInfo.PodName, Success: true})
		}
	}

	// broadcast and collect acks from other pods
	acks, publishErr := s.publishRefreshAndCollectAcks(ctx, req.ServiceName, appPayload, totalPods)
	if publishErr != nil {
		log.Printf("publish refresh failed: %v", publishErr)
		writeError(w, http.StatusInternalServerError, fmt.Errorf("publish failed: %w", publishErr))
		return
	}

	results, confirmed := mergeRefreshResults(results, acks, pods)

	writeJSON(w, http.StatusOK, map[string]any{
		"service":   req.ServiceName,
		"total":     totalPods,
		"confirmed": confirmed,
		"pods":      results,
	})
}

// mergeRefreshResults combines local + broadcast acks with a "no response"
// entry for any pod that never acked, and counts how many succeeded.
func mergeRefreshResults(local []PodAckResult, acks []RefreshAck, pods []PodInfo) ([]PodAckResult, int) {
	results := local
	for _, ack := range acks {
		results = append(results, PodAckResult{PodName: ack.PodName, Success: ack.Success, Error: ack.Error})
	}

	responded := make(map[string]bool, len(results))
	for _, r := range results {
		responded[r.PodName] = true
	}
	for _, p := range pods {
		if !responded[p.PodName] {
			results = append(results, PodAckResult{PodName: p.PodName, Success: false, Error: "no response (timeout)"})
		}
	}

	confirmed := 0
	for _, r := range results {
		if r.Success {
			confirmed++
		}
	}

	return results, confirmed
}

// --- health ---

func (s *sidecar) handleHealth(w http.ResponseWriter, r *http.Request) {
	redisOK := s.Redis.Ping(r.Context()).Err() == nil
	appReady := s.IsReady()

	// sidecar is always alive (for k8s liveness), but reports readiness status
	writeJSON(w, http.StatusOK, map[string]any{
		"redis": redisOK,
		"app":   appReady,
	})
}
