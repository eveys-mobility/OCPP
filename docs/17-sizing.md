# 17 — Production sizing for 500–1000 CP fleets

> Hardware sizing for an `eveys/ocpp` deployment serving 500–1000
> chargers. Three configurations covering the realistic range:
> minimum (1 node), middle (2 nodes), recommended (3 nodes). All
> three target a single fleet on a single LAN; remote / multi-region
> deployments need their own write-up.
>
> **Scope note**: this doc sizes for "the gateway runs and serves
> the fleet correctly." Surviving one node going down without the
> charger fleet noticing — Postgres replication, Kafka clustering,
> Redis Sentinel — is **not in scope here** and lands as a separate
> doc when ops picks which HA trade-offs to take. The recommended
> 3-node shape is the path that's *ready* for HA without a forklift
> upgrade; it doesn't deliver HA on its own.

## What "node" means here — server vs pod

This doc uses "node," "server," "host," and "physical machine"
interchangeably — they all mean **one box with a CPU and RAM
running Linux + k8s**. One server = one k8s node.

A **pod** is a different thing: a running instance of a service.
The chart at `deploy/helm/eveys-ocpp/` defines a fixed set of
pods. The number of servers you provision changes how those pods
are distributed; it does **not** change the pod count itself.

**Pod count for 500–1000 CP** (set by the Helm chart, the same
across all three sizing tiers below):

| Pod | Replicas | Why |
|---|---|---|
| eveys-ocpp gateway | 2 | One can drain on a deploy while the other serves; consistent-hash on `cp_id` (ADR-0007) keeps charger stickiness |
| Envoy edge | 2 | Same — sits in front of the gateway and needs the same redundancy |
| Postgres 16 | 1 | Single instance for v0 (HA explicitly deferred) |
| Redis 7 | 1 | Same |
| Kafka (KRaft mode) | 1 | Same |
| ClickHouse | 1 | Same |
| ClickHouse ingestor | 1 | Single Kafka consumer is enough |
| **Total pods** | **9** | |

**Servers** are the question. Below, "1-node config", "2-node
config", "3-node config" mean **how many physical servers those 9
pods are spread across** — not how many k8s namespaces, not how
many control-plane replicas, not how many pod replicas.

What deliberately does NOT happen: **all services in one pod**.
That's a k8s anti-pattern (services would crash together, scale
together, get scheduled together). Each service is its own pod;
the question is only how many physical hosts the pods land on.

```
                                Pod placement, 2-server config

  ┌──────────────────────────┐                ┌──────────────────────────┐
  │ Server 1 (app, 8 GB)     │                │ Server 2 (data, 16 GB)   │
  │                          │                │                          │
  │  ┌──────────────────┐    │                │  ┌──────────────────┐   │
  │  │ gateway pod #1   │    │                │  │ gateway pod #2   │   │
  │  └──────────────────┘    │                │  └──────────────────┘   │
  │  ┌──────────────────┐    │                │  ┌──────────────────┐   │
  │  │ Envoy pod #1     │    │                │  │ Envoy pod #2     │   │
  │  └──────────────────┘    │                │  └──────────────────┘   │
  │                          │                │  ┌──────────────────┐   │
  │  k8s + OS                │                │  │ Postgres         │   │
  │                          │                │  └──────────────────┘   │
  │                          │                │  ┌──────────────────┐   │
  │                          │                │  │ Redis            │   │
  │                          │                │  └──────────────────┘   │
  │                          │                │  ┌──────────────────┐   │
  │                          │                │  │ Kafka            │   │
  │                          │                │  └──────────────────┘   │
  │                          │                │  ┌──────────────────┐   │
  │                          │                │  │ ClickHouse       │   │
  │                          │                │  └──────────────────┘   │
  │                          │                │  ┌──────────────────┐   │
  │                          │                │  │ ingestor         │   │
  │                          │                │  └──────────────────┘   │
  │                          │                │  k8s + OS                │
  └──────────────────────────┘                └──────────────────────────┘
              ▲                                            ▲
              │                                            │
              └────────────────────────────────────────────┘
                          same LAN; charger traffic
                          hits Envoy via the LB IP
```

A **3-server config** moves the data-plane pods (Postgres / Redis
/ Kafka / ClickHouse / ingestor) onto a dedicated Server 3, leaves
gateway + Envoy split across Servers 1 + 2, and uses Server 3 as
the third k8s control-plane vote (proper quorum). Same 9 pods,
just spread differently.

A **1-server config** runs all 9 pods on one box. Same Helm chart,
same pod count; k8s schedules them all to the single available
host.

So the sizing question reduces to: **how many physical servers do
you provision, given that the pod count is fixed at 9?**

## Traffic shape at fleet scale

OCPP load is steady and predictable, not bursty. Per-charger
per-minute traffic at the protocol's defaults:

| Action | Cadence | Notes |
|---|---|---|
| Heartbeat | ~1 / minute | Default 60 s heartbeat interval |
| StatusNotification | 0–2 / minute | Event-driven; spikes on connector state transitions |
| MeterValues | 2–4 / minute (during active session) | Default sample interval ~30 s; only when a session is in progress |
| BootNotification | per reconnect | One on first boot, one per WS reconnect |
| Authorize / StartTransaction / StopTransaction | per session | Hours apart for typical fleets |

**For 1000 CP at steady state**: roughly 50–100 OCPP CALLs/sec
across the whole fleet. That's nothing for modern hardware — **the
cost dimensions that actually matter are different**:

1. **WebSocket fan-out** — 1000 long-lived TLS connections held
   open 24/7. ~50–200 KB per connection in the gateway process →
   **~200 MB total**.
2. **Postgres connection pool** + the Authorize hot path. Worst
   case ~500 Authorize/min during a regional power-up event;
   trivial under normal load.
3. **Kafka throughput** — MeterValues during active sessions.
   100 active sessions × 4 samples/min × ~500 bytes ≈ **3 KB/s**.
4. **ClickHouse storage growth** — telemetry firehose accumulates
   ~10 GB/year per 1000 CP at default sample rate. Cheap.
5. **Reconnect storm** (the one place CPU briefly matters) — 1000
   chargers all reconnecting at once on a deploy or fleet-wide
   network blip. The graceful-drain mechanism (PR #43) and per-pod
   readiness probe stagger this, but the worst-case spike is still
   the planning constraint for CPU headroom.

Everything below this section is sized **against (5)** as the load
peak, with (1) as the always-on baseline.

## Per-component memory budget at 1000 CP

This is the all-in steady-state RAM budget when every component
runs on the same host (the "co-tenant" shape — what 1-node and the
data-plane node of 2-node and 3-node configs all use).

| Component | RAM | Notes |
|---|---|---|
| eveys-ocpp gateway pod | 1 GB | ~1k WS sockets × ~200 KB + Python runtime + asyncio overhead |
| Envoy edge pod | 256 MB | Connection state for 1k upstream sockets |
| Postgres 16 | 4 GB | `shared_buffers=1GB`; working set fits in RAM at this scale |
| Redis 7 | 512 MB | Online registry (~100 KB), idempotency cache (5 min TTL), per-charger rate-limit buckets, Authorize cache (30 s TTL) |
| Kafka (KRaft mode, single broker) | 2 GB | JVM heap + page cache for the topic logs |
| ClickHouse | 4 GB | Mark cache + uncompressed cache + query reservation pool |
| ClickHouse ingestor sidecar | 256 MB | Single-process Kafka → CH consumer |
| **Workload subtotal** | **~12 GB** | All `eveys/ocpp` components running |
| k8s control plane (k3s) | 1 GB | Substantially less than full kubeadm |
| kubelet + CNI + kube-proxy | 500 MB | Per-node; multiplies in 2/3-node shapes |
| OS + monitoring agent | 1 GB | Linux + node-exporter + Promtail or equivalent |
| Headroom for spikes | 1 GB | Reconnect storms, ClickHouse query peaks, PG cache warming |
| **All-in per host** | **~15.5 GB** | Need 16 GB to leave breathing room |

This budget assumes **co-tenancy** — every component on the same
node. Splitting data plane onto a dedicated host doesn't help until
the fleet is well past 10k CP; until then it just doubles your
hardware cost.

## CPU sizing

OCPP is I/O-bound; CPU only matters during reconnect storms and
ClickHouse query peaks. Per-component peak under load:

| Component | Steady CPU | Peak CPU (storm / heavy query) |
|---|---|---|
| eveys-ocpp gateway | 0.3 | 1.0 |
| Envoy edge | 0.2 | 0.5 |
| Postgres 16 | 0.3 | 1.0 (Authorize storm) |
| Redis 7 | 0.1 | 0.3 |
| Kafka | 0.5 | 1.5 (replication + ingestor catch-up) |
| ClickHouse | 0.5 | 2.0 (heavy MeterValues query) |
| ClickHouse ingestor | 0.2 | 0.5 |
| OS + k8s | 0.3 | 0.5 |
| **Steady total** | **~2.4 CPU** | |
| **Peak total** | | **~7.3 CPU** |

Steady fits comfortably in 4 CPU. Peak wants 8 CPU per host to
avoid throttling under simultaneous storm + query — which is **why
the recommended config below specifies 8 CPU / node**, not 4.

## Storage

| Subsystem | Annual growth at 1000 CP | Notes |
|---|---|---|
| ClickHouse `cp_meter` (MeterValues firehose) | ~8 GB / year | Bulk of the volume |
| ClickHouse `cp_status` / `cp_boot` / `tx_started` | ~2 GB / year combined | Smaller per-row tables |
| Postgres transaction history | ~5 GB / year | One row per session × 1000 CP × ~30/day |
| Kafka topic retention (default 7 d) | ~1 GB at any time | Sliding window |
| OS + container images + logs | ~20 GB | One-time-ish |
| **5-year working set** | **~75 GB** | Then ADR-0013 retention + cold tier kicks in |

**500 GB SSD per node** covers ~5 years of telemetry on a single
host before any tier-to-cold-storage decision is forced. **NVMe
strongly preferred** for Postgres + ClickHouse — both are
random-IO-bound at peak.

## Network

For 500–1000 CP on the same LAN: **1 GbE is sufficient**. Peak
inter-node traffic is ClickHouse + Kafka replication; in a 3-node
co-tenant shape the cross-node bandwidth tops out around 50 Mbps
under a heavy storm. **2.5 GbE or 10 GbE** is over-spec'd for the
fleet size but cheap insurance against a hot follow-up workload
(a sibling service that wants to consume Kafka at line rate).

## Three configurations

### **Minimum — 1 node**

**1 × (8 CPU / 16 GB RAM / 500 GB NVMe SSD).**

Total: **8 CPU / 16 GB / 500 GB**.

What it runs: everything, single replica each. `k3s` solo (no
control-plane overhead worth speaking of). Gateway + Envoy + PG +
Redis + Kafka + ClickHouse + ingestor on the same kernel, sharing
one kernel page cache.

**What it survives**:
- Restart of any single component (k8s reschedules the pod; charger
  reconnects).
- Day-to-day operation at 500–1000 CP with normal reconnect rates.

**What it doesn't survive**:
- The host going down. Whole fleet goes offline until the host is
  back. **Recovery is operator-visible** — there's no automatic
  failover.
- Major OS kernel upgrade requiring a reboot (~minutes downtime).
- Disk failure if the SSD is single. RAID 1 on the storage layer
  (~+50% disk cost) is the right answer if uptime matters.

**Use case**: dev fleet, internal staging, an operator running 500
CP behind a single physical machine in a charging garage. Cheapest
real production posture. **Pre-validated** by every existing
compose-smoke run — the compose stack is functionally identical.

### **Middle — 2 servers**

**Asymmetric** — Server 2 carries the data-plane pods so it needs
more RAM than Server 1.

- **Server 1 (app)**: 4 CPU / **8 GB** RAM / 100 GB SSD
- **Server 2 (data)**: 4 CPU / **16 GB** RAM / 500 GB NVMe SSD

Total: **8 CPU / 24 GB / 600 GB**.

The **headroom variant** ups Server 1 to 16 GB too — useful if a
sibling workload (Prometheus, Grafana, Loki, the simulator) might
land there. **Total: 8 CPU / 32 GB / 600 GB**. Costs ~25% more;
worth it if you'd otherwise run the observability stack elsewhere.

What it runs (per the diagram in the "server vs pod" section
above):
- **Server 1 (app)**: 1× gateway pod, 1× Envoy pod.
- **Server 2 (data)**: 1× gateway pod, 1× Envoy pod, plus
  Postgres, Redis, Kafka, ClickHouse, ClickHouse ingestor.

Splitting one gateway + one Envoy onto **each** server (rather
than putting both gateway pods on Server 1) is what gives the WS
layer real HA: a reboot of Server 1 still leaves Server 2's
gateway pod serving the fleet via consistent-hash on `cp_id` (per
ADR-0007). Putting both gateway pods on Server 1 would mean a
Server-1 reboot takes the whole fleet offline — which is the
1-server config's failure mode at twice the cost.

**The data plane is single-instance** — Postgres / Redis / Kafka
/ ClickHouse each run as one pod on Server 2.

The asymmetric memory ask (Server 1 = 8 GB, Server 2 = 16 GB) is
because Server 2 carries all the data-plane RAM cost on top of
its share of the WS layer.

**What it survives**:
- Gateway pod crash → traffic flows to the sibling on the other
  node. Charger reconnect ≤ 3 s.
- Node A going down → all charger sockets drop, but data is intact.
  When Node A comes back, the fleet reconnects.

**What it doesn't survive** (the honest part):
- **Node B (data plane) going down**. PG / Redis / Kafka / CH all
  on one host. Whole gateway is unusable until Node B is back.
- **k8s control-plane quorum**. Two-node clusters have no
  arbitration — if the nodes can see each other but the cluster
  thinks they can't, scheduling decisions deadlock. **In practice
  run k3s with a single control-plane node and the second as an
  agent only** — that's stable but means losing the control plane
  node loses scheduling.

**Use case**: gateway HA matters (a single-pod restart for a deploy
shouldn't take the fleet offline), but operator accepts that data-
plane host loss is a manual recovery. **This is the natural
upgrade from "1 node" once a single-pod restart causing visible
charger reconnects becomes a problem.**

The size choice within the 2-node tier:
- **2 × 8 GB**: data node will be tight under load. Workable if the
  fleet stays near 500 CP. Saves money.
- **2 × 16 GB**: comfortable up to 1000+ CP. The extra cost is
  justified by not having to think about it again until 5k CP.

### **Recommended — 3 nodes**

**3 × (4 CPU / 16 GB RAM / 500 GB NVMe SSD)** — or **3 × (8 CPU /
16 GB)** if your reconnect storms hit hard.

Total: **12 CPU / 48 GB / 1.5 TB** (or 24 CPU at the larger CPU
tier).

What it runs (today, no HA on data plane):
- **Node A (app)**: 1× gateway pod, 1× Envoy pod.
- **Node B (app)**: 1× gateway pod, 1× Envoy pod.
- **Node C (data)**: Postgres, Redis, Kafka, ClickHouse, ingestor.

Same WS-layer HA as the 2-node config. **The 3rd node is for k8s
arbitration** — a 3-node cluster has proper control-plane quorum,
so a single node going down doesn't deadlock scheduling.

**What it survives** (today):
- Same as 2-node: gateway pod loss is transparent; node A or B
  loss is a brief reconnect.
- **Node C (data) loss is still operator-visible** — no HA on the
  data plane yet. This is the single largest pre-staged cost
  saving deferred to the future fail-safe doc.

**What it gets you for free** (the reason this is "recommended"):
- **Drop-in path to fail-safe** without buying more hardware.
  When ops picks the HA trade-offs:
  - Postgres → Patroni primary on Node C, replicas on A and B.
  - Kafka → 3 brokers, one per node, replication-factor=3.
  - Redis → Sentinel with 3 voters across nodes.
  - ClickHouse → ReplicatedMergeTree across A and B (Node C still
    primary).
  All within the existing 16 GB / 4 CPU per node — co-tenancy
  budget already accounts for it.
- **Real `kubectl drain` for OS upgrades**. Pull a node, k8s
  reschedules, fleet doesn't notice.
- **Headroom for an in-cluster Prometheus + Grafana + Loki stack**
  if you want observability dashboards on the same hardware.

**Use case**: production-realistic deployment that's ready to grow
into HA without a forklift hardware upgrade. The right answer if
the fleet is going to grow beyond 1000 CP and the team will
eventually want fail-safety.

## Comparison summary

| Config | Hardware total | Gateway HA | Data-plane HA | Survives 1 server loss | k8s quorum | Cost order |
|---|---|---|---|---|---|---|
| 1 server | 8 CPU / 16 GB / 500 GB | No (one host = one gateway pod's home) | No | No | N/A | 1× |
| 2 servers | 8 CPU / 24 GB / 600 GB | Yes (1 gateway pod + 1 Envoy pod per server) | No | App-server yes; data-server no | Imperfect | ~1.5× |
| 3 servers | 12 CPU / 48 GB / 1.5 TB | Yes | Not yet — ready to bolt on | App-server yes; data-server no | Yes | 3× |

## Network requirements (all configs)

| Item | Why |
|---|---|
| 1 GbE per node | Sufficient for 500–1000 CP; 2.5 / 10 GbE is over-spec'd |
| Static internal IPs | k8s networking expects stable addresses |
| 1 free IP for the LoadBalancer Service | Envoy's external endpoint via klipper-lb / kube-vip |
| `/etc/hosts` mapping that LB IP to a hostname | Otherwise TLS cert validation breaks (cert-manager won't issue for a bare IP) |
| NTP synced | Postgres + Kafka misbehave under clock skew |
| Charger network reachable to the LB IP on `:443` | wss:// terminates at Envoy |

## Software prep (all configs)

| Item | Why |
|---|---|
| Ubuntu 22.04 LTS or Debian 12 | Tested baseline; any modern Linux works |
| Docker / containerd | k3s defaults to containerd |
| `k3s` for single-node and small clusters | Substantially less control-plane RAM than full kubeadm |
| `helm` ≥ 3.16 on the operator's laptop | For deploying `deploy/helm/eveys-ocpp/` |
| Cert source picked | cert-manager + Let's Encrypt is the usual answer; or a manually-managed TLS Secret |

## Growth path past 1000 CP

These thresholds shape the **next** sizing rev, not this one:

- **At ~3000 CP**: gateway pod count grows past 4. Consider splitting
  ClickHouse off the data node onto its own host — the heavy queries
  start touching gateway latency.
- **At ~5000 CP**: Kafka cluster rebalancing across 3+ brokers is
  worth the complexity. Postgres connection pool tuning starts to
  matter.
- **At ~10000 CP**: dedicated DB hardware (3 + 2 split: 3 app nodes
  + 2 dedicated DB nodes). The compose-style "everything together"
  shape stops paying off.

## What this doc deliberately does NOT cover

- **Failover automation**. Patroni configuration, Sentinel quorum,
  Kafka replication-factor tuning — separate doc, lands when ops
  picks which HA trade-offs to take.
- **Backup + restore**. Postgres PITR, ClickHouse `clickhouse-backup`
  to S3 (per ADR-0013) — separate ops concern.
- **Cross-region or DR-site replication**. Out of scope for a
  single-LAN deployment; ADR territory.
- **Cost in dollars**. Hardware pricing varies by vendor / region /
  contract; the CPU/RAM/disk numbers above translate to whatever
  list price your supplier charges.
