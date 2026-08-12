package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequireToken(t *testing.T) {
	called := false
	ok := func(w http.ResponseWriter, r *http.Request) { called = true }

	cases := []struct {
		name     string
		cfgToken string
		sent     string
		want     int
		wantCall bool
	}{
		{"valid token passes", "s3cret", "s3cret", http.StatusOK, true},
		{"wrong token rejected", "s3cret", "nope", http.StatusUnauthorized, false},
		{"missing token rejected", "s3cret", "", http.StatusUnauthorized, false},
		{"prefix of token rejected", "s3cret", "s3c", http.StatusUnauthorized, false},
		{"no token configured skips check", "", "", http.StatusOK, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called = false
			s := &Sidecar{Config: Config{InMemToken: tc.cfgToken}}
			req := httptest.NewRequest("GET", "/api/services", nil)
			if tc.sent != "" {
				req.Header.Set("x-inmem-token", tc.sent)
			}
			rec := httptest.NewRecorder()
			s.requireToken(ok)(rec, req)

			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d", rec.Code, tc.want)
			}
			if called != tc.wantCall {
				t.Errorf("handler called = %v, want %v", called, tc.wantCall)
			}
		})
	}
}

func TestMatchKeysByInfix(t *testing.T) {
	names := []string{"driver:profile:123", "rider:profile:456", "config:fare", "DRIVER:vehicle:9"}

	cases := []struct {
		name  string
		infix string
		want  []string
	}{
		{"matches substring anywhere", "profile", []string{"driver:profile:123", "rider:profile:456"}},
		{"case-insensitive", "driver", []string{"driver:profile:123", "DRIVER:vehicle:9"}},
		{"no match returns none", "nonexistent", nil},
		{"matches mid-token", "are", []string{"config:fare"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := matchKeysByInfix(names, tc.infix)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("got[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// health must stay reachable without a token so k8s probes keep working.
func TestHealthNeedsNoToken(t *testing.T) {
	s := &Sidecar{Config: Config{InMemToken: "s3cret"}}
	req := httptest.NewRequest("GET", "/api/health", nil)
	rec := httptest.NewRecorder()

	defer func() {
		// handleHealth pings Redis, which is nil here — a panic still proves
		// the request got past auth, which is what this test is about.
		if r := recover(); r != nil && rec.Code == http.StatusUnauthorized {
			t.Fatal("health returned 401")
		}
	}()
	s.Routes().ServeHTTP(rec, req)

	if rec.Code == http.StatusUnauthorized {
		t.Errorf("health returned 401, want it open to unauthenticated probes")
	}
}

// every /api route except health must be behind the token check.
func TestAllAPIRoutesRequireToken(t *testing.T) {
	s := &Sidecar{Config: Config{InMemToken: "s3cret"}}
	routes := []struct{ method, path string }{
		{"POST", "/api/registerKey"},
		{"GET", "/api/services"},
		{"GET", "/api/pods"},
		{"GET", "/api/keys"},
		{"POST", "/api/pod/get"},
		{"POST", "/api/refresh"},
	}

	for _, rt := range routes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			req := httptest.NewRequest(rt.method, rt.path, nil)
			rec := httptest.NewRecorder()
			s.Routes().ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401 for tokenless request", rec.Code)
			}
		})
	}
}
