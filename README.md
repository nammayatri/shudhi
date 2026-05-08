# Shudhi

See, inspect, and clear in-memory caches across all your services and pods — from one dashboard.

Every microservice caches things in memory (local maps, Guava, Caffeine, etc). Once it's in memory, it's invisible — you can't see what's cached, you can't inspect values, and you can't clear a stale entry without restarting the pod. Multiply that by 10 services and 50 pods, and you're flying blind.

Shudhi fixes this. Deploy it as a sidecar, implement 3 simple endpoints in your app, and you get:

- **Visibility** — see every cached key across every pod, from one place
- **Inspection** — read the actual cached value from any specific pod
- **Invalidation** — clear a key (or all keys) across all pods of a service, instantly

```
    Your services with in-memory caches
    ┌─────────┐  ┌─────────┐  ┌─────────┐
    │ App A   │  │ App B   │  │ App C   │
    │ (cache) │  │ (cache) │  │ (cache) │
    └────┬────┘  └────┬────┘  └────┬────┘
         │            │            │
    ┌────┴────┐  ┌────┴────┐  ┌────┴────┐
    │Sidecar A│  │Sidecar B│  │Sidecar C│    ← Shudhi sidecars
    └────┬────┘  └────┬────┘  └────┬────┘
         │            │            │
         └────────────┼────────────┘
                      │
              Redis (coordination)
```

Redis is used internally for sidecar coordination (discovery, message passing). Your app never talks to Redis — it only talks to its local sidecar over HTTP.

## Quick Start

```bash
APP_URL=http://localhost:8080 REDIS_URL=localhost:6379 go run .
```

| Env Var | Default | Description |
|---------|---------|-------------|
| `APP_URL` | `http://localhost:8080` | Your app's base URL |
| `SIDECAR_PORT` | `8900` | Sidecar listen port |
| `REDIS_URL` | `localhost:6379` | Redis address (for sidecar coordination) |
| `POD_IP` | `127.0.0.1` | Pod IP (k8s downward API) |
| `INMEM_TOKEN` | _(empty)_ | Shared auth token between sidecar and app |

## Integrating Your App

Your app keeps its own in-memory cache as-is. No changes to your caching logic. You just expose 3 endpoints so the sidecar can talk to your cache, and optionally make 1 call to register keys for dashboard visibility.

### 1. Tell the sidecar who you are (required)

```
GET /internal/inMem/serverInfo
→ { "serviceName": "rider-app", "podName": "rider-app-7b4f8d6c9-x2k4m" }
```

Called once at startup. The sidecar uses this to identify itself in the network.

### 2. Return a cached value on demand (required)

```
POST /internal/inMem/get
← { "key": "RouteByRouteId:config-id:route-123" }
→ { "found": true, "value": { "vehicleType": "SUV" } }
```

Look up the key in your in-memory cache and return the value.

### 3. Clear cache entries on demand (required)

```
POST /internal/inMem/refresh
← { "keyInfix": "RouteByRouteId" }   // or null to clear all
→ 200 OK
```

Delete any in-memory entries whose key contains the given `keyInfix`. If `null`, clear your entire cache.

### 4. Register cached keys (optional, enables dashboard visibility)

Whenever your app puts something in its in-memory cache, tell the sidecar:

```
POST http://localhost:8900/api/registerKey
{
  "keyName": "RouteByRouteId:config-id:route-123",
  "keySchema": null,
  "ttlInSeconds": 3600
}
```

This makes the key show up in the dashboard so you can browse and inspect it. Without this, get/refresh still work — you just won't see the key listed.

> If `INMEM_TOKEN` is set, the sidecar sends it as `x-inmem-token` header on all calls to your app. Use it to verify requests come from a trusted sidecar.

## Sidecar API

All endpoints work from any sidecar — the dashboard only needs to reach one to interact with any service's in-memory cache.

| Method | Endpoint | What it does |
|--------|----------|-------------|
| `GET` | `/api/services` | List all services with registered caches |
| `GET` | `/api/pods?service=X` | List live pods for a service |
| `GET` | `/api/keys?service=X&pod=Y` | List cached keys (pod optional) |
| `POST` | `/api/pod/get` | Read a cached value from a specific pod's memory |
| `POST` | `/api/refresh` | Clear cache entries across all pods of a service |
| `POST` | `/api/registerKey` | Register a cache key (called by your app) |
| `GET` | `/api/health` | Health check |

## How It Works

**Your app's in-memory cache is the source of truth.** The sidecar never caches data itself — it's a thin coordination layer that lets you reach into any pod's memory from anywhere.

**Startup**: The sidecar calls your app's `/serverInfo` to learn its identity, then announces itself to other sidecars via Redis. A heartbeat keeps the registration alive (60s TTL, refreshed every 30s). On shutdown, it deregisters immediately.

**Inspecting a value**: When you click "Get" on a key in the dashboard, the sidecar routes the request to the correct pod — either via direct HTTP or via a pub/sub relay if the pod isn't directly reachable. The target pod's sidecar calls the app's `/get` endpoint, reads the value from the app's in-memory cache, and returns it.

**Clearing cache**: When you clear a key, the sidecar broadcasts to all pods of that service. Each pod's sidecar receives the message and calls its local app's `/refresh` endpoint, which removes the entry from the app's in-memory cache. The stale data is gone from every pod within seconds.

**Key registration**: When your app registers a key, the sidecar stores the metadata (key name, schema, TTL) so the dashboard can list it. This is just a registry for browsing — the actual cached data always lives in your app's memory.

**Redis is only for coordination** — sidecar discovery, message routing, and key metadata. Pub/sub messages are fire-and-forget (never stored). The only persistent keys are pod liveness (60s TTL) and key metadata (3-day TTL), both self-cleaning. Nothing piles up.

## Use Cases

**"A config changed in the DB, clear it from all pods"**
You updated a pricing config, but 12 pods still serve the old value from their in-memory cache. Hit Clear in the dashboard and every pod drops the stale entry instantly. No restarts needed.

**"Why is this user seeing stale data?"**
Open the dashboard, find the service, pick the pod, click Get on the cache key. You're looking at the exact value sitting in that pod's memory right now. No port-forwarding, no kubectl exec.

**"We need cache visibility across 5 microservices"**
Each team has their own service with their own in-memory caches. Deploy Shudhi as a sidecar to each, and the dashboard shows all services, all pods, all cached keys in one place.

**"Rolling out a feature flag change"**
Feature flags are cached in memory. Instead of waiting for TTL expiry, clear the flag's cache key and every pod picks up the new value within seconds.

**"Debugging cache inconsistency between pods"**
Pod A returns one value, Pod B returns another. Use the dashboard to Get the value from each pod and see exactly what diverged — without reproducing the issue or adding temporary logging.
