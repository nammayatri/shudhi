package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	AppURL      string
	SidecarPort string
	RedisURL    string
	PodIP       string
}

func LoadConfig() Config {
	return Config{
		AppURL:      envOrDefault("APP_URL", "http://localhost:8080"),
		SidecarPort: envOrDefault("SIDECAR_PORT", "8900"),
		RedisURL:    envOrDefault("REDIS_URL", "localhost:6379"),
		PodIP:       envOrDefault("POD_IP", "127.0.0.1"),
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// App identity, fetched once at startup
type AppInfo struct {
	ServiceName string `json:"serviceName"`
	PodName     string `json:"podName"`
}

// Sidecar holds all shared state
type Sidecar struct {
	Config  Config
	Redis   *redis.Client
	AppInfo AppInfo
	HTTP    *http.Client
}

func NewSidecar(cfg Config) (*Sidecar, error) {
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisURL})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	s := &Sidecar{
		Config: cfg,
		Redis:  rdb,
		HTTP:   &http.Client{Timeout: 5 * time.Second},
	}

	info, err := s.fetchAppInfo()
	if err != nil {
		return nil, fmt.Errorf("app serverInfo: %w", err)
	}
	s.AppInfo = info
	log.Printf("connected to app: service=%s pod=%s", info.ServiceName, info.PodName)

	return s, nil
}

func (s *Sidecar) fetchAppInfo() (AppInfo, error) {
	resp, err := s.HTTP.Get(s.Config.AppURL + "/internal/inMem/serverInfo")
	if err != nil {
		return AppInfo{}, err
	}
	defer resp.Body.Close()
	var info AppInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return AppInfo{}, err
	}
	return info, nil
}

// Redis key helpers
func (s *Sidecar) podKey() string {
	return fmt.Sprintf("inmem:pod:%s:%s", s.AppInfo.ServiceName, s.AppInfo.PodName)
}

func (s *Sidecar) keysKey() string {
	return fmt.Sprintf("inmem:keys:%s:%s", s.AppInfo.ServiceName, s.AppInfo.PodName)
}

func (s *Sidecar) sidecarURL() string {
	return fmt.Sprintf("http://%s:%s", s.Config.PodIP, s.Config.SidecarPort)
}

// pub/sub channels
func pubsubChannel(serviceName string) string {
	return fmt.Sprintf("inmem:%s", serviceName)
}

func podRequestChannel(serviceName, podName string) string {
	return fmt.Sprintf("inmem:req:%s:%s", serviceName, podName)
}
