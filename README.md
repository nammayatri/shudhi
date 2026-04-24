# Shudhi

A sidecar for managing in-memory caches across services. Deploy alongside any app that implements the [InMem Management Protocol](#app-contract), and get cross-service cache visibility, targeted key lookups, and coordinated cache invalidation — all through a shared Redis.

## Architecture

```
                    ┌──────────────────────────┐
                    │          Redis            │
                    │                           │
                    │  inmem:pod:<svc>:<pod>     │  pod liveness (TTL 60s)
                    │  inmem:keys:<svc>:<pod>    │  key registry (TTL 3d)
                    │  inmem:<svc>      (pubsub) │  broadcast refresh
                    │  inmem:req:<svc>:<pod>     │  targeted get (pubsub RPC)
                    └────────┬─────────────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
         Sidecar A      Sidecar B      Sidecar C
         (svc: rider)   (svc: rider)   (svc: driver)
              │              │              │
         App Pod A      App Pod B      App Pod C
```

Every sidecar connects to the same Redis. Discovery (list services, pods, keys) works from any sidecar. Interaction (get value, refresh) routes to the right place via Redis pub/sub — no direct sidecar-to-sidecar HTTP required.

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `APP_URL` | `http://localhost:8080` | Local app's base URL |
| `SIDECAR_PORT` | `8900` | Port the sidecar listens on |
| `REDIS_URL` | `localhost:6379` | Redis address |
| `POD_IP` | `127.0.0.1` | Pod IP (from k8s downward API) |

## Running

```bash
# Binary
APP_URL=http://localhost:8080 REDIS_URL=localhost:6379 go run .

# Docker
docker run -e APP_URL=http://localhost:8080 -e REDIS_URL=redis:6379 ghcr.io/<owner>/shudhi
```

## API

All endpoints are served by the sidecar. The dashboard (or any client) only needs to reach one sidecar to interact with any service.

### Discovery (reads from Redis, works from any sidecar)

#### `GET /api/services`

List all registered services.

```json
{ "services": ["rider-app", "driver-app", "payment-service"] }
```

#### `GET /api/pods?service=rider-app`

List live pods for a service.

```json
{
  "pods": [
    { "podName": "rider-app-7b4f8d6c9-x2k4m", "sidecarUrl": "http://10.0.3.42:8900" }
  ]
}
```

#### `GET /api/keys?service=rider-app&pod=<optional>`

List registered cache keys. If `pod` is omitted, returns keys across all pods.

```json
{
  "keys": [
    {
      "keyName": "RouteByRouteId:config-id:route-123",
      "keySchema": null,
      "ttlInSeconds": 3600,
      "registeredAt": "2026-04-24T10:30:00Z",
      "podName": "rider-app-7b4f8d6c9-x2k4m"
    }
  ]
}
```

### Interaction (routes to the correct service/pod)

#### `POST /api/pod/get`

Get a cached value from a specific pod. Tries direct HTTP to the target sidecar, falls back to Redis pub/sub RPC.

```json
// Request
{
  "serviceName": "rider-app",
  "podName": "rider-app-7b4f8d6c9-x2k4m",
  "key": "RouteByRouteId:config-id:route-123"
}

// Response (proxied from app)
{
  "found": true,
  "value": { "vehicleType": "SUV", "category": "premium" }
}
```

#### `POST /api/refresh`

Invalidate cache entries across all pods of a service. Publishes to the service's Redis pub/sub channel.

```json
// Request
{
  "serviceName": "rider-app",
  "keyPrefix": "RouteByRouteId"    // null to clear all
}

// Response
{ "published": true, "service": "rider-app" }
```

### App-facing

#### `POST /api/registerKey`

Called by the app to register a cached key with the sidecar.

```json
{
  "keyName": "RouteByRouteId:config-id:route-123",
  "keySchema": { "type": "object", "properties": { "vehicleType": { "type": "string" } } },
  "ttlInSeconds": 3600
}
```

Stored in Redis hash `inmem:keys:<serviceName>:<podName>` with a 3-day TTL.

### Infra

#### `GET /api/health`

```json
{ "redis": true, "app": true }
```

Returns `503` if either Redis or the app is unreachable.

## App Contract

The app must expose these endpoints under `/internal/inMem/`:

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `GET` | `/internal/inMem/serverInfo` | Returns `{ "serviceName": "...", "podName": "..." }` |
| `POST` | `/internal/inMem/get` | Returns `{ "found": bool, "value": json }` for a given key |
| `POST` | `/internal/inMem/refresh` | Deletes cached entries by prefix (or all) |

The sidecar calls `/serverInfo` once at startup to learn its identity. `/get` and `/refresh` are called on demand.

## Pod Liveness

Each sidecar registers itself in Redis with a 60s TTL and refreshes every 30s. On graceful shutdown (`SIGTERM`), it deletes its key immediately. On crash, the key auto-expires within 60s. If the key exists, the pod is alive — no filtering logic needed.

## Redis Keys

| Key Pattern | Type | TTL | Purpose |
|-------------|------|-----|---------|
| `inmem:pod:<svc>:<pod>` | STRING | 60s | Pod liveness + sidecar URL |
| `inmem:keys:<svc>:<pod>` | HASH | 3 days | Registered cache keys |
| `inmem:<svc>` | PUBSUB | — | Broadcast channel (refresh) |
| `inmem:req:<svc>:<pod>` | PUBSUB | — | Targeted request channel (get) |
| `inmem:reply:<pod>:<nonce>` | PUBSUB | — | Ephemeral reply channel for pub/sub RPC |
