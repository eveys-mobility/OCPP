# Multi-pod and routing

**Audience.** Anyone scaling the gateway past one replica, or debugging why a command ended up on a different pod than expected.

**What this answers.** How Envoy hashes chargers to pods, how the gateway routes commands when the receiving pod doesn't own the charger, what happens during a rolling update, and why `cp.offline` events don't fire spuriously during reconnect storms.

> The configuration knobs for this concept are in [`../reference/configuration.md`](../reference/configuration.md) under `cross_pod_bus` and `redis`. This page is the architectural reasoning behind those knobs.

---

## The problem

The gateway holds a long-lived WebSocket per charger. That socket lives on exactly one pod. When your backend says "RemoteStart charger X", the REST/gRPC call lands on **any pod** behind your load balancer — usually *not* the one holding X's socket.

There are three ways to handle this in a multi-pod design:

1. **Make every pod know about every charger.** Each pod stores `cp_id → socket` in a shared store; route through a proxy. Doesn't work — sockets are not shareable.
2. **Pin charger sockets to one pod with sticky routing.** Then the question becomes "how do I find that pod from another pod" — which is solvable.
3. **Sidestep the problem with one pod.** Doesn't scale.

The gateway picks (2). Two mechanisms make it work: Envoy ring-hash routing at the edge, and a Redis-backed registry + pub/sub bus across pods.

---

## Envoy ring-hash: chargers stick to pods

Envoy sits in front of the gateway pods as the WSS terminator. Its upstream cluster is the **headless** Service for the gateway (so each pod has its own DNS A record). The cluster's load-balancing policy is `RING_HASH` keyed on the URL path — which contains `cp_id`.

```yaml
# Simplified excerpt of Envoy's upstream cluster config
clusters:
  - name: eveys-ocpp-gateway
    lb_policy: RING_HASH
    consistent_hashing_lb_config:
      use_hostname_for_hashing: false
    # ... headless service discovery
    load_balancing_policy:
      policies:
        - typed_extension_config:
            name: envoy.load_balancing_policies.ring_hash
            typed_config:
              minimum_ring_size: 1024
```

Combined with the route's `hash_policy` keyed on `:path`, charger `CP_X`'s WebSocket upgrade lands on the same pod every time — *as long as the pod set is stable*. When pods come and go, the consistent-hash ring rebalances minimally: a single pod join/leave moves ~1/N of chargers, not all of them.

**What this means in practice.** A charger reconnects after a brief network blip → lands on the same pod, same registry key, no state churn. A pod crashes → its chargers redistribute across the remaining pods. A new pod scales out → it absorbs a fraction of new connections; existing connections stay where they are.

---

## The online registry: who owns whom

When a charger's socket opens on pod `P`, `P` writes:

```
SET cp:online:<cp_id> <pod_id> EX <ttl>
```

`ttl` is a few multiples of the heartbeat interval — so even if a pod crashes without cleaning up, its keys expire on their own.

Every heartbeat refreshes the TTL. Every command originating on a different pod reads this key to find the owner.

The gauge `eveys_ocpp_registry_online_chargers` reflects this map's size from each pod's view (sum it across pods for the fleet truth; differences between pods indicate split-brain in Redis or stale TTLs).

---

## Cross-pod dispatch: the request bus

Pod `A` receives `POST /commands/remote-start` for charger `X`. `X` is owned by pod `B`. What happens:

1. `A` looks up `cp:online:X` in Redis → `pod_id=B`.
2. `A` publishes a request envelope to the Redis pub/sub channel `bus:requests:B`:

   ```json
   {
     "request_id": "8a3f2c40-3b8e-4d0a-9b62-7a3d5d1e9fa1",
     "rpc": "RemoteStart",
     "cp_id": "X",
     "payload": { ... }
   }
   ```

3. `A` opens an inproc future keyed on `request_id` with a 35-second deadline.
4. `B`'s subscriber picks up the message, dispatches the OCPP CALL on `X`'s socket, awaits the reply, then publishes the response to `bus:responses:A` (same `request_id`).
5. `A`'s subscriber wakes the inproc future; `A` returns the result to the original REST/gRPC caller.

Total overhead in the steady state: ~5–10 ms on top of the OCPP round-trip. Metrics:

- `eveys_ocpp_grpc_dispatch_route_total{route="local"|"remote"}` — how often the request was same-pod vs cross-pod.
- `eveys_ocpp_bus_request_latency_seconds` — the cross-pod hop itself.
- `eveys_ocpp_bus_inflight` — current in-flight bus requests on each pod.

**Why pub/sub, not direct gRPC pod-to-pod.** A handful of reasons:

- Redis is already in the stack (registry + caches). No new dependency.
- Pub/sub doesn't need pod-to-pod discovery — both pods just talk to the same Redis.
- Failover semantics are simple: if `B` is gone, the response never comes back; `A`'s future times out at 35 s and returns `CHARGER_OFFLINE` to the caller.

The trade-off: Redis is in the critical path of every cross-pod call. Use a Redis with adequate capacity and a sane connection pool size.

---

## Online and offline events: avoiding spurious fires

A subtle problem: charger `X` is on pod `B`. Pod `B` is restarted. `X` reconnects to pod `C` (different ring slot post-rebalance) within seconds. Naive logic would fire:

- `B` sees its socket close → publishes `cp.offline` for `X`.
- `C` sees a new socket open → publishes `cp.online` for `X`.

But from a backend's perspective, `X` was never offline — the socket churn was a platform-internal hiccup. Spurious offline events are operationally annoying.

The fix: **compare-and-delete** on the registry key, **only fire `cp.offline` if you still owned the key**.

```python
# On socket close, on pod B:
was_ours = redis.eval("""
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
  else
    return 0
  end
""", keys=["cp:online:X"], args=[B_pod_id])

if was_ours:
    publish("cp.offline", {"cp_id": "X", "pod_id": B_pod_id, "reason": "clean"})
```

If `C` has already overwritten the key (because `X` reconnected during `B`'s shutdown), `was_ours` is 0 and no offline event fires. Backends only see `cp.offline` when a charger is genuinely off.

---

## Rolling updates: zero-drop, mostly

The chart configures rolling updates with `maxUnavailable: 0` and a `terminationGracePeriodSeconds` that exceeds the gateway's grace window. The sequence per pod is:

1. New pod `P_new` comes up, fails liveness until ready, then passes readiness.
2. Old pod `P_old` receives `SIGTERM`. It flips `/api/v1/ready` to 503 so Envoy stops sending new connections.
3. `P_old` keeps servicing existing sockets and inflight requests for up to `EVEYS_OCPP_SHUTDOWN_GRACE_PERIOD_SECONDS`.
4. After the grace window, `P_old` closes remaining sockets cleanly. Chargers reconnect; the consistent-hash ring distributes them across remaining pods.
5. Kubernetes deletes `P_old` and moves on to the next pod.

Net effect: charger sockets see at most one disconnect-and-reconnect during the whole rollout. Active charging sessions are not interrupted — the session continues, the socket churn is transparent to the user.

The metric to watch during a rollout: `eveys_ocpp_ws_connections_active`. If you see a deeper dip than expected, something else is going on (Envoy didn't see the pod's readiness flip, or the charger fleet isn't reconnecting promptly).

---

## What can still go wrong

### Redis as single point of failure

Every cross-pod call routes through Redis pub/sub. A Redis outage means cross-pod dispatch fails (the pod that received the REST call returns `CHARGER_OFFLINE` after the timeout). Same-pod calls keep working.

Mitigation: high-availability Redis (Sentinel or Cluster). The gateway accepts both modes via `EVEYS_OCPP_REDIS_URL`.

### Pod-affinity mismatch during scale-down

When the autoscaler removes pods, the ring rebalances. The chargers that *were* on the removed pod reconnect to whichever pod the new ring slot points to. There's a few-seconds gap during which a command for one of those chargers returns `CHARGER_OFFLINE` (the registry key expired on the removed pod, and the new pod hasn't seen a reconnect yet).

Mitigation: conservative scale-down (the `stabilizationWindowSeconds: 300` recommendation in [`../guides/deploy-to-production.md`](../guides/deploy-to-production.md)).

### Split-brain in Redis

If Redis itself splits brain (rare with HA configured correctly), two pods may both think they own the same charger. The compare-and-delete on offline is defensive against this — only the pod that wrote its own value can delete it — but commands could route to either pod. The OCPP wire layer would catch this (the wrong pod has no socket; CALL fails) but the user-facing message is a confusing `CHARGER_TIMEOUT` rather than an offline error.

This is a rare configuration bug rather than a normal failure mode.

### Heartbeat-registry reclaims

The `eveys_ocpp_heartbeat_registry_reclaims_total` counter tracks "a heartbeat arrived on pod P, but the registry said the charger was on pod Q". Non-zero values indicate a previous owner that didn't clean up its key (typically a pod that crashed without finalising). The gateway transparently reclaims the key for the new owner; you should expect this counter to be small and bursty, not zero.

---

## Where to go from here

- The other half of "duplicate-protection": [`idempotency-and-replay.md`](./idempotency-and-replay.md).
- How a single message threads through everything described here: [`how-ocpp-flows-work.md`](./how-ocpp-flows-work.md).
- Operational view of pod churn: [`../guides/operate.md`](../guides/operate.md).
