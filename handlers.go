package main

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxBroadcastRetries = 3

// how long a pod's registered-key hash survives without being touched
const keysRegistryTTL = 3 * 24 * time.Hour

func (s *Sidecar) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/registerKey", s.requireToken(s.handleRegisterKey))
	mux.HandleFunc("GET /api/services", s.requireToken(s.requireReady(s.handleServices)))
	mux.HandleFunc("GET /api/pods", s.requireToken(s.requireReady(s.handlePods)))
	mux.HandleFunc("GET /api/keys", s.requireToken(s.requireReady(s.handleKeys)))
	mux.HandleFunc("POST /api/pod/get", s.requireToken(s.requireReady(s.handlePodGet)))
	mux.HandleFunc("POST /api/refresh", s.requireToken(s.requireReady(s.handleRefresh)))
	// health stays open — k8s probes have no token
	mux.HandleFunc("GET /api/health", s.handleHealth)
	return mux
}

// requireToken rejects requests that don't carry the shared x-inmem-token.
// If INMEM_TOKEN is unset the check is skipped, so existing deployments keep
// working until they set one (main logs a warning in that case).
func (s *Sidecar) requireToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Config.InMemToken != "" {
			got := r.Header.Get("x-inmem-token")
			if !hmac.Equal([]byte(got), []byte(s.Config.InMemToken)) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(map[string]string{"error": "invalid or missing x-inmem-token"})
				return
			}
		}
		next(w, r)
	}
}

func (s *Sidecar) requireReady(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.IsReady() {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"error": "sidecar not connected to app yet"})
			return
		}
		next(w, r)
	}
}

// --- registerKey: app registers a cached key ---

type RegisterKeyReq struct {
	KeyName      string           `json:"keyName"`
	KeySchema    *json.RawMessage `json:"keySchema"`
	TTLInSeconds int              `json:"ttlInSeconds,omitempty"`
}

func (s *Sidecar) handleRegisterKey(w http.ResponseWriter, r *http.Request) {
	if !s.IsReady() {
		// accept silently — don't break the app, keys will re-register later
		w.WriteHeader(http.StatusAccepted)
		return
	}

	var req RegisterKeyReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
	pipe.Expire(ctx, key, keysRegistryTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// --- services: list all service names ---

func (s *Sidecar) handleServices(w http.ResponseWriter, r *http.Request) {
	services, err := s.scanServices(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"services": services})
}

func (s *Sidecar) scanServices(ctx context.Context) ([]string, error) {
	seen := map[string]bool{}
	var cursor uint64
	for {
		keys, next, err := s.Redis.Scan(ctx, cursor, "inmem:pod:*", 100).Result()
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

func (s *Sidecar) handlePods(w http.ResponseWriter, r *http.Request) {
	svc := r.URL.Query().Get("service")
	if svc == "" {
		http.Error(w, "service param required", http.StatusBadRequest)
		return
	}

	pods, err := s.scanPods(r.Context(), svc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"pods": pods})
}

type PodInfo struct {
	PodName    string `json:"podName"`
	SidecarURL string `json:"sidecarUrl"`
}

func (s *Sidecar) scanPods(ctx context.Context, svc string) ([]PodInfo, error) {
	var pods []PodInfo
	var cursor uint64
	prefix := fmt.Sprintf("inmem:pod:%s:", svc)
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

func (s *Sidecar) handleKeys(w http.ResponseWriter, r *http.Request) {
	svc := r.URL.Query().Get("service")
	if svc == "" {
		http.Error(w, "service param required", http.StatusBadRequest)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
	json.NewEncoder(w).Encode(map[string]any{"keys": result, "source": "registry"})
}

func (s *Sidecar) proxyKeysToApp(ctx context.Context, w http.ResponseWriter, r *http.Request, svc, pod, pattern, limit, offset string) {
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
	if pod == s.AppInfo.PodName && svc == s.AppInfo.ServiceName {
		resp, err := s.doGet(ctx, s.Config.AppURL+targetPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("app unreachable: %v", err), http.StatusBadGateway)
			return
		}
		defer drainClose(resp)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Prevent proxy loops
	if r.Header.Get("X-Shudhi-Proxied") != "" {
		http.Error(w, "proxy loop detected", http.StatusLoopDetected)
		return
	}

	// Proxy to target pod's sidecar
	podRedisKey := fmt.Sprintf("inmem:pod:%s:%s", svc, pod)
	targetURL, err := s.Redis.Get(ctx, podRedisKey).Result()
	if err != nil {
		http.Error(w, "pod not found", http.StatusNotFound)
		return
	}

	// Forward full /api/keys request to target sidecar (which will call its local app)
	fwdQ := r.URL.Query()
	fwdURL := targetURL + "/api/keys?" + fwdQ.Encode()
	proxyReq, _ := http.NewRequestWithContext(ctx, "GET", fwdURL, nil)
	proxyReq.Header.Set("X-Shudhi-Proxied", "true")
	if s.Config.InMemToken != "" {
		proxyReq.Header.Set("x-inmem-token", s.Config.InMemToken)
	}
	resp, err := s.ProxyHTTP.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("sidecar unreachable: %v", err), http.StatusBadGateway)
		return
	}
	defer drainClose(resp)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (s *Sidecar) getKeysForPod(ctx context.Context, svc, pod string) ([]map[string]any, error) {
	hashKey := fmt.Sprintf("inmem:keys:%s:%s", svc, pod)
	entries, err := s.Redis.HGetAll(ctx, hashKey).Result()
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for keyName, meta := range entries {
		var m map[string]any
		json.Unmarshal([]byte(meta), &m)
		m["keyName"] = keyName
		m["podName"] = pod
		out = append(out, m)
	}
	return out, nil
}

// --- pod/get: query a specific pod for a key's value ---

type PodGetReq struct {
	ServiceName string `json:"serviceName"`
	PodName     string `json:"podName"`
	Key         string `json:"key"`
}

func (s *Sidecar) handlePodGet(w http.ResponseWriter, r *http.Request) {
	var req PodGetReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// if it's this pod, call local app directly
	if req.PodName == s.AppInfo.PodName && req.ServiceName == s.AppInfo.ServiceName {
		s.proxyGetToApp(ctx, w, req.Key)
		return
	}

	// prevent infinite proxy loops
	if r.Header.Get("X-Shudhi-Proxied") != "" {
		http.Error(w, "proxy loop detected", http.StatusLoopDetected)
		return
	}

	// try direct HTTP to target sidecar
	podRedisKey := fmt.Sprintf("inmem:pod:%s:%s", req.ServiceName, req.PodName)
	targetURL, err := s.Redis.Get(ctx, podRedisKey).Result()
	if err == nil {
		body, _ := json.Marshal(PodGetReq{ServiceName: req.ServiceName, PodName: req.PodName, Key: req.Key})
		proxyReq, _ := http.NewRequestWithContext(ctx, "POST", targetURL+"/api/pod/get", strings.NewReader(string(body)))
		proxyReq.Header.Set("Content-Type", "application/json")
		proxyReq.Header.Set("X-Shudhi-Proxied", "true")
		if s.Config.InMemToken != "" {
			proxyReq.Header.Set("x-inmem-token", s.Config.InMemToken)
		}
		resp, httpErr := s.ProxyHTTP.Do(proxyReq)
		if httpErr == nil {
			defer drainClose(resp)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			return
		}
		log.Printf("direct HTTP to %s failed, falling back to pubsub: %v", req.PodName, httpErr)
	}

	// fallback: pub/sub RPC
	result, err := s.pubsubGet(ctx, req.ServiceName, req.PodName, req.Key)
	if err != nil {
		http.Error(w, fmt.Sprintf("pod unreachable: %v", err), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

func (s *Sidecar) proxyGetToApp(ctx context.Context, w http.ResponseWriter, key string) {
	body, _ := json.Marshal(map[string]string{"key": key})
	resp, err := s.doPost(ctx, s.Config.AppURL+"/internal/inMem/get", "application/json", strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, fmt.Sprintf("app unreachable: %v", err), http.StatusBadGateway)
		return
	}
	defer drainClose(resp)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// --- refresh: publish to target service's broadcast channel ---

type RefreshReq struct {
	ServiceName string  `json:"serviceName"`
	KeyInfix    *string `json:"keyInfix"`
}

type PodAckResult struct {
	PodName string `json:"podName"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

func (s *Sidecar) handleRefresh(w http.ResponseWriter, r *http.Request) {
	var req RefreshReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ServiceName == "" {
		http.Error(w, "serviceName required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	appPayload, _ := json.Marshal(map[string]any{"keyInfix": req.KeyInfix})

	// get pod count for the target service so we know how many acks to expect
	pods, _ := s.scanPods(ctx, req.ServiceName)
	totalPods := len(pods)

	// build per-pod results
	var results []PodAckResult

	// if this sidecar is part of the target service, refresh locally first.
	// registry entries come out before the app is told to clear, and go back
	// in if the clear doesn't land — a stale listing beats a missing one.
	if req.ServiceName == s.AppInfo.ServiceName {
		snap := s.deregisterKeys(ctx, req.KeyInfix)

		resp, err := s.doPost(ctx, s.Config.AppURL+"/internal/inMem/refresh", "application/json",
			strings.NewReader(string(appPayload)))
		if err != nil {
			log.Printf("local refresh failed (will still broadcast): %v", err)
			s.restoreKeys(ctx, snap)
			results = append(results, PodAckResult{PodName: s.AppInfo.PodName, Success: false, Error: err.Error()})
		} else {
			applied := resp.StatusCode < 300
			status := resp.StatusCode
			drainClose(resp)
			if applied {
				results = append(results, PodAckResult{PodName: s.AppInfo.PodName, Success: true})
			} else {
				s.restoreKeys(ctx, snap)
				log.Printf("local refresh returned %d (will still broadcast)", status)
				results = append(results, PodAckResult{
					PodName: s.AppInfo.PodName,
					Success: false,
					Error:   fmt.Sprintf("app returned %d", status),
				})
			}
		}
	}

	// broadcast and collect acks from other pods
	acks, publishErr := s.publishRefreshAndCollectAcks(ctx, req.ServiceName, appPayload, totalPods)
	if publishErr != nil {
		log.Printf("publish refresh failed: %v", publishErr)
		http.Error(w, fmt.Sprintf("publish failed: %v", publishErr), http.StatusInternalServerError)
		return
	}

	// merge acks into results
	for _, ack := range acks {
		results = append(results, PodAckResult{PodName: ack.PodName, Success: ack.Success, Error: ack.Error})
	}

	// figure out which pods didn't respond
	responded := map[string]bool{}
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"service":   req.ServiceName,
		"total":     totalPods,
		"confirmed": confirmed,
		"pods":      results,
	})
}

// keysSnapshot holds registry entries pulled ahead of a refresh, so they can go
// back if the refresh never lands. Zero value means nothing was removed.
type keysSnapshot struct {
	fields map[string]string
	ttl    time.Duration
}

// deregisterKeys removes cleared keys from this pod's registry hash, so the
// dashboard stops listing keys that are no longer cached. Matching mirrors the
// /api/keys pattern filter: case-insensitive substring. A nil or empty infix
// means "everything is being cleared", so every field goes.
//
// Called *before* the app is asked to clear; the returned snapshot goes back
// via restoreKeys if that clear fails. Only ever touches this sidecar's own
// hash — peer pods do the same for themselves when they apply the broadcast.
func (s *Sidecar) deregisterKeys(ctx context.Context, keyInfix *string) keysSnapshot {
	hashKey := s.keysKey()

	entries, err := s.Redis.HGetAll(ctx, hashKey).Result()
	if err != nil {
		log.Printf("deregister keys: reading %s failed: %v", hashKey, err)
		return keysSnapshot{}
	}
	if len(entries) == 0 {
		return keysSnapshot{}
	}

	all := make([]string, 0, len(entries))
	for name := range entries {
		all = append(all, name)
	}
	names := all
	if keyInfix != nil && *keyInfix != "" {
		names = matchKeysByInfix(all, *keyInfix)
	}
	if len(names) == 0 {
		return keysSnapshot{}
	}

	// keep the remaining TTL so a restore doesn't silently extend the entry's life
	ttl, err := s.Redis.TTL(ctx, hashKey).Result()
	if err != nil || ttl <= 0 {
		ttl = keysRegistryTTL
	}

	snap := keysSnapshot{fields: make(map[string]string, len(names)), ttl: ttl}
	for _, name := range names {
		snap.fields[name] = entries[name]
	}

	// HDel drops the hash itself once the last field goes, so no separate Del
	if err := s.Redis.HDel(ctx, hashKey, names...).Err(); err != nil {
		log.Printf("deregister %d keys from %s failed: %v", len(names), hashKey, err)
		return keysSnapshot{} // nothing came out, so there's nothing to put back
	}
	return snap
}

// restoreKeys puts a snapshot back after a refresh that didn't land. It detaches
// from the caller's context so a cancelled request can't skip the compensating
// write and leave the registry missing keys that are still cached.
func (s *Sidecar) restoreKeys(ctx context.Context, snap keysSnapshot) {
	if len(snap.fields) == 0 {
		return
	}
	hashKey := s.keysKey()

	vals := make([]any, 0, len(snap.fields)*2)
	for name, meta := range snap.fields {
		vals = append(vals, name, meta)
	}
	ttl := snap.ttl
	if ttl <= 0 {
		ttl = keysRegistryTTL
	}

	restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	pipe := s.Redis.Pipeline()
	pipe.HSet(restoreCtx, hashKey, vals...)
	pipe.Expire(restoreCtx, hashKey, ttl)
	if _, err := pipe.Exec(restoreCtx); err != nil {
		log.Printf("restoring %d keys to %s failed: %v", len(snap.fields), hashKey, err)
		return
	}
	log.Printf("refresh failed — restored %d keys to %s", len(snap.fields), hashKey)
}

func matchKeysByInfix(names []string, keyInfix string) []string {
	infix := strings.ToLower(keyInfix)
	var matched []string
	for _, name := range names {
		if strings.Contains(strings.ToLower(name), infix) {
			matched = append(matched, name)
		}
	}
	return matched
}

// --- health ---

func (s *Sidecar) handleHealth(w http.ResponseWriter, r *http.Request) {
	redisOK := s.Redis.Ping(r.Context()).Err() == nil
	appReady := s.IsReady()

	// sidecar is always alive (for k8s liveness), but reports readiness status
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"redis": redisOK,
		"app":   appReady,
	})
}
