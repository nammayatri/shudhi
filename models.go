package main

import "encoding/json"

type RegisterKeyReq struct {
	KeyName      string           `json:"keyName"`
	KeySchema    *json.RawMessage `json:"keySchema"`
	TTLInSeconds int              `json:"ttlInSeconds,omitempty"`
}

type PodInfo struct {
	PodName    string `json:"podName"`
	SidecarURL string `json:"sidecarUrl"`
}

type PodGetReq struct {
	ServiceName string `json:"serviceName"`
	PodName     string `json:"podName"`
	Key         string `json:"key"`
}

type RefreshReq struct {
	ServiceName string  `json:"serviceName"`
	KeyInfix    *string `json:"keyInfix"`
}

type PodAckResult struct {
	PodName string `json:"podName"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type PubSubMessage struct {
	Action    string          `json:"action"`
	Payload   json.RawMessage `json:"payload"`
	OriginPod string          `json:"originPod"`
	Timestamp string          `json:"timestamp"`
	ReplyTo   string          `json:"replyTo,omitempty"`
}

type RefreshAck struct {
	PodName string `json:"podName"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}
