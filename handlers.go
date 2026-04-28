package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const maxBroadcastRetries = 3

func (s *Sidecar) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/registerKey", s.handleRegisterKey)
	mux.HandleFunc("GET /api/services", s.requireReady(s.handleServices))
	mux.HandleFunc("GET /api/pods", s.requireReady(s.handlePods))
	mux.HandleFunc("GET /api/keys", s.requireReady(s.handleKeys))
	mux.HandleFunc("POST /api/pod/get", s.requireReady(s.handlePodGet))
	mux.HandleFunc("POST /api/refresh", s.requireReady(s.handleRefresh))
	mux.HandleFunc("GET /api/health", s.handleHealth)
	return mux
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
	pipe.Expire(ctx, key, 3*24*time.Hour)
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
	pod := r.URL.Query().Get("pod") // optional

	ctx := r.Context()
	var result []map[string]any

	if pod != "" {
		entries, err := s.getKeysForPod(ctx, svc, pod)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result = entries
	} else {
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
	}
	json.NewEncoder(w).Encode(map[string]any{"keys": result})
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
		body, _ := json.Marshal(map[string]string{"key": req.Key})
		proxyReq, _ := http.NewRequestWithContext(ctx, "POST", targetURL+"/api/pod/get", strings.NewReader(string(body)))
		proxyReq.Header.Set("Content-Type", "application/json")
		proxyReq.Header.Set("X-Shudhi-Proxied", "true")
		if s.Config.InMemToken != "" {
			proxyReq.Header.Set("x-inmem-token", s.Config.InMemToken)
		}
		resp, httpErr := s.HTTP.Do(proxyReq)
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
	KeyInfix   *string `json:"keyInfix"`
}

func (s *Sidecar) handleRefresh(w http.ResponseWriter, r *http.Request) {
	var req RefreshReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ServiceName == "" {
		http.Error(w, "serviceName required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	appPayload, _ := json.Marshal(map[string]any{"keyInfix": req.KeyInfix})

	var localErr error
	// if this sidecar is part of the target service, also refresh locally
	if req.ServiceName == s.AppInfo.ServiceName {
		resp, err := s.doPost(ctx, s.Config.AppURL+"/internal/inMem/refresh", "application/json",
			strings.NewReader(string(appPayload)))
		if err != nil {
			localErr = err
			log.Printf("local refresh failed (will still broadcast): %v", err)
		} else {
			drainClose(resp)
		}
	}

	// always attempt broadcast even if local refresh failed —
	// other pods still need to be refreshed
	publishErr := s.publishRefresh(ctx, req.ServiceName, appPayload)
	if publishErr != nil {
		log.Printf("publish refresh failed: %v", publishErr)
	}

	// report combined status
	if localErr != nil && publishErr != nil {
		http.Error(w, fmt.Sprintf("local: %v, publish: %v", localErr, publishErr), http.StatusInternalServerError)
		return
	}
	if publishErr != nil {
		http.Error(w, fmt.Sprintf("local refreshed but broadcast failed: %v", publishErr), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := map[string]any{"published": true, "service": req.ServiceName}
	if localErr != nil {
		resp["localError"] = localErr.Error()
	}
	json.NewEncoder(w).Encode(resp)
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
