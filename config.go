package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	AppURL      string
	SidecarPort string
	RedisURL    string
	RedisDB     int
	PodIP       string
	InMemToken  string
}

func LoadConfig() Config {
	db, _ := strconv.Atoi(envOrDefault("REDIS_DB", "0"))
	return Config{
		AppURL:      envOrDefault("APP_URL", "http://localhost:8080"),
		SidecarPort: envOrDefault("SIDECAR_PORT", "8900"),
		RedisURL:    envOrDefault("REDIS_URL", "localhost:6379"),
		RedisDB:     db,
		PodIP:       envOrDefault("POD_IP", "127.0.0.1"),
		InMemToken:  os.Getenv("INMEM_TOKEN"),
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type AppInfo struct {
	ServiceName string `json:"serviceName"`
	PodName     string `json:"podName"`
}

type Sidecar struct {
	Config  Config
	Redis   *redis.Client
	AppInfo AppInfo
	HTTP    *http.Client
	ready   atomic.Bool // true once app info is fetched and registered
}

func NewSidecar(cfg Config) *Sidecar {
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisURL, DB: cfg.RedisDB})
	return &Sidecar{
		Config: cfg,
		Redis:  rdb,
		HTTP:   &http.Client{Timeout: 5 * time.Second},
	}
}

// WaitForApp retries fetching app info until it succeeds or ctx is cancelled.
// Once connected, registers in Redis and starts heartbeat + pubsub.
func (s *Sidecar) WaitForApp(ctx context.Context) {
	for {
		if err := s.tryConnect(ctx); err != nil {
			log.Printf("waiting for app: %v (retrying in 10s)", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				continue
			}
		}
		return
	}
}

func (s *Sidecar) tryConnect(ctx context.Context) error {
	if err := s.Redis.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	info, err := s.fetchAppInfo(ctx)
	if err != nil {
		return fmt.Errorf("app serverInfo: %w", err)
	}
	s.AppInfo = info
	log.Printf("connected to app: service=%s pod=%s", info.ServiceName, info.PodName)

	if err := s.Register(ctx); err != nil {
		return fmt.Errorf("register: %w", err)
	}

	s.ready.Store(true)
	go s.Heartbeat(ctx)
	go s.SubscribePubSub(ctx)
	return nil
}

func (s *Sidecar) IsReady() bool {
	return s.ready.Load()
}

func (s *Sidecar) fetchAppInfo(ctx context.Context) (AppInfo, error) {
	resp, err := s.doGet(ctx, s.Config.AppURL+"/internal/inMem/serverInfo")
	if err != nil {
		return AppInfo{}, err
	}
	defer drainClose(resp)
	var info AppInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return AppInfo{}, err
	}
	return info, nil
}

func (s *Sidecar) podKey() string {
	return fmt.Sprintf("inmem:pod:%s:%s", s.AppInfo.ServiceName, s.AppInfo.PodName)
}

func (s *Sidecar) keysKey() string {
	return fmt.Sprintf("inmem:keys:%s:%s", s.AppInfo.ServiceName, s.AppInfo.PodName)
}

func (s *Sidecar) sidecarURL() string {
	return fmt.Sprintf("http://%s:%s", s.Config.PodIP, s.Config.SidecarPort)
}

func (s *Sidecar) doPost(ctx context.Context, url, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	if s.Config.InMemToken != "" {
		req.Header.Set("x-inmem-token", s.Config.InMemToken)
	}
	return s.HTTP.Do(req)
}

func (s *Sidecar) doGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	if s.Config.InMemToken != "" {
		req.Header.Set("x-inmem-token", s.Config.InMemToken)
	}
	return s.HTTP.Do(req)
}

// drainClose drains and closes a response body to allow connection reuse.
func drainClose(resp *http.Response) {
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func pubsubChannel(serviceName string) string {
	return fmt.Sprintf("inmem:%s", serviceName)
}

func podRequestChannel(serviceName, podName string) string {
	return fmt.Sprintf("inmem:req:%s:%s", serviceName, podName)
}
