package main

import (
	"errors"
	"net/http"
)

func (s *sidecar) Routes() http.Handler {
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

func (s *sidecar) requireReady(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.IsReady() {
			writeError(w, http.StatusServiceUnavailable, errors.New("sidecar not connected to app yet"))
			return
		}
		next(w, r)
	}
}
