package main

import (
	"encoding/json"
	"io"
	"net/http"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

// writeRawJSON writes a pre-serialized JSON body (e.g. from a pubsub reply).
func writeRawJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}

// copyJSON relays an upstream JSON response's status and body as-is.
func copyJSON(w http.ResponseWriter, resp *http.Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// isLocalPod reports whether svc/pod refers to this sidecar's own app instance.
func (s *sidecar) isLocalPod(svc, pod string) bool {
	return pod == s.AppInfo.PodName && svc == s.AppInfo.ServiceName
}

// isProxied reports whether this request was already forwarded by another sidecar.
func isProxied(r *http.Request) bool {
	return r.Header.Get("X-Shudhi-Proxied") != ""
}
