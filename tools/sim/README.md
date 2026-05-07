# `eveys-ocpp-sim` — virtual charger fleet

Standalone CLI that runs N concurrent virtual OCPP 1.6 chargers
against a target gateway. Drives the realistic lifecycle:
BootNotification → Authorize → StartTransaction → MeterValues × N →
StopTransaction → Disconnect → reconnect.

Used by E4-6 (load test rig) and E4-7 (reconnect-storm test); also a
dev affordance — `python -m tools.sim --count 10 --duration 60` is
the new "is the gateway alive?" smoke.

## Why no `[project.scripts]` entry

`tools/sim/` lives **outside** the `src/eveys_ocpp/` source tree and
is **not** in `[tool.hatch.build.targets.wheel] packages`. A
`[project.scripts]` entry pointing at `tools.sim:main` would resolve
in a dev checkout but be a dangling pointer in any production wheel.
Use `python -m tools.sim ...` from a dev checkout instead.

## Quickstart

```bash
# Bring up a local gateway first (compose stack)
make compose-up

# Then run the simulator
python -m tools.sim --count 10 --duration 60 --target ws://localhost:19000

# Behaviour profiles
python -m tools.sim --count 100 --profile idle      # connect + heartbeat only
python -m tools.sim --count 50  --profile churning  # disconnect/reconnect storm
python -m tools.sim --count 200 --profile realistic # ~1 txn / charger / hour
```

## Output

Per-second status line on stderr:

```
[t=  42s remaining=  18s] connected=998 boots=1000 txns=37 errors=0
```

Final summary on stderr at exit.

## Profiles

| Profile      | Heartbeat | Transaction start | Disconnect |
| ------------ | --------- | ----------------- | ---------- |
| `realistic`  | 60 s      | 1 / charger / hour | never      |
| `idle`       | 60 s      | never             | never      |
| `churning`   | 60 s      | never             | ~1 / minute |

Custom shapes: build a `BehaviourProfile` directly and pass it to
`Fleet(FleetConfig(profile=...))`.

## Acceptance scope

The simulator's design target is **1k chargers on one laptop** for
E4-6's local-stack baseline. Multi-node simulator runs (10k+) are a
Phase 4 follow-up if the load test demands more than one box can
produce.

`wss://` is **not supported** — the gateway terminates plaintext
`ws://` per ADR-0026; production puts Envoy in front for TLS. The
simulator matches the gateway's wire shape.
