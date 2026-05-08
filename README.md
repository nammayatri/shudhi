# Shudhi

A sidecar for managing in-memory caches across services. Deploy it alongside your app to get cache visibility, key lookups, and coordinated invalidation — all through Redis.

```
           Redis (shared)
               │
    ┌──────────┼──────────┐
    │          │          │
 Sidecar A  Sidecar B  Sidecar C
    │          │          │
 App Pod A  App Pod B  App Pod C
```

## Quick Start

```bash
APP_URL=http://localhost:8080 REDIS_URL=localhost:6379 go run .
```

| Env Var | Default | Description |
|---------|---------|-------------|
| `APP_URL` | `http://localhost:8080` | Your app's base URL |
| `SIDECAR_PORT` | `8900` | Sidecar listen port |
| `REDIS_URL` | `localhost:6379` | Redis address |
| `POD_IP` | `127.0.0.1` | Pod IP (k8s downward API) |
| `INMEM_TOKEN` | _(empty)_ | Shared auth token between sidecar and app |

## Integrating Your App

Your app needs to expose 3 endpoints and make 1 optional call:

### 1. Tell the sidecar who you are (required)

```
GET /internal/inMem/serverInfo
→ { "serviceName": "rider-app", "podName": "rider-app-7b4f8d6c9-x2k4m" }
```

Called once at startup. The sidecar uses this to register itself in Redis.

### 2. Return a cached value on demand (required)

```
POST /internal/inMem/get
← { "key": "RouteByRouteId:config-id:route-123" }
→ { "found": true, "value": { "vehicleType": "SUV" } }
```

### 3. Clear cache entries on demand (required)

```
POST /internal/inMem/refresh
← { "keyInfix": "RouteByRouteId" }   // or null to clear all
→ 200 OK
```

Delete any cached entries whose key contains the given `keyInfix`. If `null`, clear everything.

### 4. Register cached keys (optional, enables dashboard visibility)

Whenever your app caches something, tell the sidecar:

```
POST http://localhost:8900/api/registerKey
{
  "keyName": "RouteByRouteId:config-id:route-123",
  "keySchema": null,
  "ttlInSeconds": 3600
}
```

This makes the key visible in the dashboard. If the sidecar isn't ready yet, the call is silently accepted.

> If `INMEM_TOKEN` is set, the sidecar sends it as `x-inmem-token` header on all calls to your app. Use it to verify requests come from a trusted sidecar.

## Sidecar API

All endpoints work from any sidecar — the dashboard only needs to reach one.

| Method | Endpoint | What it does |
|--------|----------|-------------|
| `GET` | `/api/services` | List all services |
| `GET` | `/api/pods?service=X` | List live pods for a service |
| `GET` | `/api/keys?service=X&pod=Y` | List registered cache keys (pod optional) |
| `POST` | `/api/pod/get` | Get a cached value from a specific pod |
| `POST` | `/api/refresh` | Clear cache entries across all pods of a service |
| `POST` | `/api/registerKey` | Register a cache key (called by your app) |
| `GET` | `/api/health` | Health check (`{ "redis": bool, "app": bool }`) |

## How It Works

**Startup**: The sidecar calls your app's `/serverInfo` once to learn its service name and pod name. It then registers itself in Redis (`inmem:pod:<svc>:<pod>` with a 60s TTL) and subscribes to the service's pub/sub channel. A heartbeat refreshes the TTL every 30s. On `SIGTERM`, the key is deleted immediately; on crash, it auto-expires within 60s.

**Getting a value**: When the dashboard (or any client) asks for a key from a specific pod, the sidecar looks up the target pod's URL from Redis and proxies the request directly. If direct HTTP fails (network policy, pod restarting), it falls back to a Redis pub/sub RPC — publishes a request to the target pod's channel and waits for a reply on an ephemeral channel.

**Refreshing / clearing cache**: A refresh request publishes a message to the service's broadcast channel in Redis. Every sidecar subscribed to that channel receives it and forwards the clear command to its local app. The originating pod skips the broadcast (it already cleared locally). Retries up to 3 times with backoff if the app returns an error.

**Key registration**: When your app calls `/api/registerKey`, the sidecar writes the key metadata to a Redis hash (`inmem:keys:<svc>:<pod>`, 3-day TTL). The dashboard reads these hashes to show what's cached where. This is the only way keys appear in the dashboard — if you don't register, the sidecar still works for get/refresh, you just lose visibility.

**Nothing piles up in Redis.** Pub/Sub messages are fire-and-forget — delivered to active subscribers and immediately discarded. The only persistent keys are pod liveness (60s TTL) and key registrations (3-day TTL), both self-cleaning.

## Use Cases

**"A config changed in the DB, clear it from all pods"**
You updated a pricing config in the database, but 12 pods still have the old value in memory. Instead of restarting the deployment, hit the dashboard's Clear button (or call `/api/refresh`) and every pod drops the stale entry. Next request fetches fresh from DB.

**"Why is this user seeing stale data?"**
A customer reports seeing outdated information. Open the dashboard, find the service, pick the user's pod, and click Get on the relevant cache key to see exactly what's in memory on that pod right now. No port-forwarding, no kubectl exec, no guesswork.

**"We need cache visibility across 5 microservices"**
Each team owns their own service with their own in-memory cache. Deploy Shudhi as a sidecar to each, point them at the same Redis, and the dashboard shows all services, all pods, all cached keys in one place. Platform team gets visibility without each service team building custom tooling.

**"Rolling out a feature flag change"**
Feature flags cached in memory across pods. Instead of waiting for TTL expiry (could be hours), trigger a targeted refresh for the flag's cache key and every pod picks up the new value within seconds.

**"Debugging cache inconsistency between pods"**
Pod A returns one value, Pod B returns another for the same key. Use the dashboard to Get the value from each pod side by side and see exactly what diverged — no need to reproduce the issue or add temporary logging.
