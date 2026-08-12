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

> If `INMEM_TOKEN` is set, the sidecar sends it as `x-inmem-token` header on all calls to your app. Use it to verify requests come from a trusted sidecar. Your app must send the same header back on `/api/registerKey` — see Authentication below.

## Sidecar API

All endpoints work from any sidecar — the dashboard only needs to reach one to interact with any service's in-memory cache.

| Method | Endpoint | What it does | Token |
|--------|----------|-------------|-------|
| `GET` | `/api/services` | List all services with registered caches | required |
| `GET` | `/api/pods?service=X` | List live pods for a service | required |
| `GET` | `/api/keys?service=X&pod=Y` | List cached keys (pod optional) | required |
| `POST` | `/api/pod/get` | Read a cached value from a specific pod's memory | required |
| `POST` | `/api/refresh` | Clear cache entries across all pods of a service | required |
| `POST` | `/api/registerKey` | Register a cache key (called by your app) | required |
| `GET` | `/api/health` | Health check | open |

## Authentication

`INMEM_TOKEN` is a single shared secret used in both directions:

- **Outbound** — the sidecar sends `x-inmem-token` on every call it makes to your app and to peer sidecars.
- **Inbound** — every `/api/*` endpoint except `/api/health` requires the same header. A missing or wrong token gets `401`. Comparison is constant-time.

`/api/health` stays open so k8s liveness/readiness probes work without credentials.

**If `INMEM_TOKEN` is unset, inbound validation is skipped entirely** and every endpoint is open — including reading cached values out of any pod and clearing caches fleet-wide. The sidecar logs a warning at startup when this is the case. Set a token in any shared environment.

> **Upgrading:** if you already run with `INMEM_TOKEN` set, your app must now send `x-inmem-token` on its `/api/registerKey` calls — previously the token was outbound-only and registration was unauthenticated. Without it, key registration gets `401` and the dashboard stops listing that pod's keys (get/refresh keep working).

The dashboard's nginx injects the token server-side via its own `INMEM_TOKEN` env var, so the browser never sees it. Note that this leaves the dashboard itself unauthenticated — anyone who can reach it can act through it. Put auth in front of the dashboard if it's exposed beyond your cluster.

## How It Works

**Your app's in-memory cache is the source of truth.** The sidecar never caches data itself — it's a thin coordination layer that lets you reach into any pod's memory from anywhere.

**Startup**: The sidecar calls your app's `/serverInfo` to learn its identity, then announces itself to other sidecars via Redis. A heartbeat keeps the registration alive (60s TTL, refreshed every 30s). On shutdown, it deregisters immediately.

**Inspecting a value**: When you click "Get" on a key in the dashboard, the sidecar routes the request to the correct pod — either via direct HTTP or via a pub/sub relay if the pod isn't directly reachable. The target pod's sidecar calls the app's `/get` endpoint, reads the value from the app's in-memory cache, and returns it.

**Clearing cache**: When you clear a key, the sidecar broadcasts to all pods of that service. Each pod's sidecar receives the message and calls its local app's `/refresh` endpoint, which removes the entry from the app's in-memory cache. The stale data is gone from every pod within seconds.

**Key registration**: When your app registers a key, the sidecar stores the metadata (key name, schema, TTL) so the dashboard can list it. This is just a registry for browsing — the actual cached data always lives in your app's memory.

**Surviving disconnects**: Redis pub/sub is fire-and-forget — a pod that's disconnected when a clear is published never receives it, and reconnecting doesn't replay it. So every refresh is also appended to a durable log (`inmem:refreshlog:<service>`, a Redis Stream capped at 1000 entries with a 24h TTL) before it's broadcast. Each sidecar tracks its position in that log and replays anything it missed — on every resubscribe, and on a 30s ticker, since go-redis reconnects the underlying connection transparently and a dropped subscription doesn't always surface as an error. A pod that blips during a clear catches up within 30s instead of serving stale cache indefinitely. Live messages carry their log position, so a refresh already applied through the broadcast is never re-applied. A freshly started pod begins at the tail of the log rather than replaying history its cache never held.

**Registry cleanup**: Registry entries are removed *before* the app is asked to clear, and put back (with their remaining TTL) if the clear doesn't land — on a transport error, a non-2xx from the app, or all pub/sub retries failing. A stale listing is better than a missing one: the key is still cached, so it should still be visible. After a pod's cache is cleared, that pod's sidecar removes the matching entries from its own registry hash, so the dashboard stops listing keys that are no longer cached. A `keyInfix` clears matching entries (case-insensitive substring, same matching as the `/api/keys` filter); no infix means the whole cache was cleared, so the pod's entire registry hash is dropped. Each sidecar only ever touches its own hash, and pod registration is untouched — the pod still appears in the dashboard, just with no keys until your app registers them again. **Keys come back only when your app calls `/api/registerKey` again**, so if you register keys once at startup, cleared keys won't be listed again until the next restart.

**Redis is only for coordination** — sidecar discovery, message routing, and key metadata. The persistent keys are pod liveness (60s TTL), key metadata (3-day TTL), and the refresh log (24h TTL, capped at 1000 entries), all self-cleaning. Nothing piles up.

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
