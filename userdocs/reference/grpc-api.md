# gRPC API reference

**Use this if you** need lower overhead than REST or you're generating client stubs from `.proto` files.

**Audience.** A developer comfortable with protobuf and gRPC clients.

**What this answers.** Every RPC, request and response messages, the proto enum surface, deadlines and errors.

> The REST API mirrors every gRPC RPC and shares the same OCPP semantics. If you're integrating once from a thin client and won't generate stubs, REST is usually simpler. gRPC is the right choice for high-fanout services that benefit from HTTP/2 multiplexing and binary framing.

---

## Service definition

```protobuf
syntax = "proto3";
package eveys.ocpp_gw.v1;

service OcppGateway { ... }
```

- **Proto file**: `proto/ocpp_gw/v1/gateway.proto` in the repository.
- **Default port**: `50051` (`EVEYS_OCPP_GRPC_PORT`).
- **Transport**: HTTP/2; TLS optional (configure via env vars).

Generate stubs in your language with whichever tooling you prefer:

```bash
# Python
python -m grpc_tools.protoc \
  --proto_path=proto \
  --python_out=. --grpc_python_out=. \
  proto/ocpp_gw/v1/gateway.proto

# Go
protoc --go_out=. --go-grpc_out=. \
  -I proto proto/ocpp_gw/v1/gateway.proto

# TypeScript
buf generate --template buf.gen.yaml proto
```

---

## Authentication

The gateway accepts a bearer token in the gRPC metadata field `authorization`, same shape as the REST header:

```python
# Python (grpclib)
from grpclib.client import Channel
from grpclib.metadata import Deadline

ch = Channel(host="gateway.example.com", port=50051)
md = [("authorization", f"Bearer {TOKEN}")]
deadline = Deadline.from_timeout(35.0)  # OCPP CALL ceiling is 30s; allow margin
```

Same CSV-of-tokens config (`EVEYS_OCPP_REST_INBOUND_TOKENS`) covers both transports.

---

## Every RPC

The OCPP semantics are identical to the REST surface — the only differences are the framing and the strongly-typed enums.

| RPC | OCPP equivalent | Notes |
|---|---|---|
| `RemoteStart` | `RemoteStartTransaction.req` | Returns `RemoteStartResponse{status}`. |
| `RemoteStop` | `RemoteStopTransaction.req` | |
| `Reset` | `Reset.req` | `type ∈ Soft / Hard`. |
| `ChangeConfiguration` | `ChangeConfiguration.req` | Status: `Accepted / Rejected / RebootRequired / NotSupported`. |
| `GetConfiguration` | `GetConfiguration.req` | Returns `configuration_key[]` + `unknown_key[]`. |
| `ClearCache` | `ClearCache.req` | |
| `TriggerMessage` | `TriggerMessage.req` | Core 6 message types. |
| `ExtendedTriggerMessage` | Security Whitepaper §4.7 | Superset of Core types (adds `LogStatusNotification`, `SignChargePointCertificate`). |
| `UnlockConnector` | `UnlockConnector.req` | |
| `ChangeAvailability` | `ChangeAvailability.req` | `connector_id = 0` → whole charger. |
| `DataTransfer` | `DataTransfer.req` (outbound) | Vendor extension hatch. |
| `GetLocalListVersion` | `GetLocalListVersion.req` | |
| `SendLocalList` | `SendLocalList.req` | Full or differential. |
| `ReserveNow` | `ReserveNow.req` | |
| `CancelReservation` | `CancelReservation.req` | |
| `GetDiagnostics` | `GetDiagnostics.req` | Followed asynchronously by `DiagnosticsStatusNotification`. |
| `UpdateFirmware` | `UpdateFirmware.req` | Followed by `FirmwareStatusNotification`. |
| `SetChargingProfile` | `SetChargingProfile.req` | |
| `ClearChargingProfile` | `ClearChargingProfile.req` | |
| `GetCompositeSchedule` | `GetCompositeSchedule.req` | |
| `GetLog` | Security Whitepaper §4.6 | `log_type ∈ DiagnosticsLog / SecurityLog`. |
| `InstallCertificate` | Security Whitepaper §4.5 | Returns the SHA-256 the operator uses to address the cert later. |
| `DeleteCertificate` | Security Whitepaper §4.10 | By SHA-256. |
| `GetInstalledCertificateIds` | Security Whitepaper §4.8 | Returns hash_data per installed cert. |
| `CertificateSigned` | Security Whitepaper §4.2 | Operator pushes a signed chain in response to a charger CSR. |
| `SignedUpdateFirmware` | Security Whitepaper §4.4 | Signed firmware update. |
| `GetChargerStatus` | (synchronous read, no OCPP CALL) | Returns the gateway's cached state for one charger. |

---

## Request/response shapes

Rather than restating every message, the protobuf file is the source of truth. A representative example:

```protobuf
message RemoteStartRequest {
  string cp_id = 1;            // required
  string id_tag = 2;           // required
  int32 connector_id = 3;      // 0 = whichever; >0 = specific
  ChargingProfile profile = 4; // optional
}

message RemoteStartResponse {
  RemoteStartStatus status = 1;   // ACCEPTED / REJECTED
}

enum RemoteStartStatus {
  REMOTE_START_STATUS_UNSPECIFIED = 0;
  REMOTE_START_STATUS_ACCEPTED = 1;
  REMOTE_START_STATUS_REJECTED = 2;
}
```

**Important about enums.** Every proto enum has an `_UNSPECIFIED = 0` zero value. The gateway rejects requests that leave required enums unset (the proto3 default would otherwise look like a valid "missing" — see [`../guides/use-the-rest-api.md`](../guides/use-the-rest-api.md) for why this matters). Always set the enum field explicitly.

For the full payload tree, generate the stubs and let your IDE walk the types.

---

## Error model

gRPC errors follow standard status codes:

| gRPC code | When |
|---|---|
| `OK` | Charger replied; result is in the response message. |
| `INVALID_ARGUMENT` | Malformed request (empty `cp_id`, unset enum, negative `connector_id`, etc.). |
| `UNAUTHENTICATED` | No bearer token. |
| `PERMISSION_DENIED` | Bearer token didn't match. |
| `NOT_FOUND` | Unknown `cp_id`, `transaction_id`, `reservation_id`. |
| `FAILED_PRECONDITION` | Charger is offline; nothing to dispatch to. |
| `DEADLINE_EXCEEDED` | Charger didn't reply within the OCPP 30-second ceiling. |
| `RESOURCE_EXHAUSTED` | Rate-limited. |
| `INTERNAL` | Unexpected; file against the gateway. |

The error `details` carry a stable `error_code` string matching the REST envelope's `error_code`.

---

## Deadlines

OCPP CALLs have a hard 30-second ceiling on the wire. Set your client-side deadline above that — 35 seconds is a sensible default — to give the gateway room to translate the timeout into `DEADLINE_EXCEEDED` cleanly.

```python
async with ch:
    stub = gateway_grpc.OcppGatewayStub(ch)
    response = await stub.RemoteStart(
        gateway_pb2.RemoteStartRequest(cp_id="CP_X", id_tag="USER_RFID_123", connector_id=1),
        timeout=35.0,
    )
```

---

## Streaming

The current API is **unary** only — every RPC is request/response. There are no server-streaming or bidirectional-streaming endpoints. If you want a feed of events, subscribe to the Kafka topics; see [`events.md`](./events.md).

---

## Cross-pod dispatch is transparent

When you call a command on the gateway, the pod that handles your call doesn't necessarily own the charger's WebSocket. The gateway looks up the owning pod in Redis and forwards the call internally. Your client never sees this — same RPC, same deadline, same response shape — but you may see a few extra milliseconds of latency on cross-pod hops.

The internals are in [`../concepts/multi-pod-and-routing.md`](../concepts/multi-pod-and-routing.md).

---

## Reflection

The gateway does not enable gRPC reflection by default. To explore the service interactively, point [`grpcurl`](https://github.com/fullstorydev/grpcurl) at your local stack with the proto file:

```bash
grpcurl -import-path proto -proto proto/ocpp_gw/v1/gateway.proto \
  -H "authorization: Bearer $TOKEN" \
  -d '{"cp_id":"CP_X","id_tag":"USER","connector_id":1}' \
  localhost:50051 eveys.ocpp_gw.v1.OcppGateway/RemoteStart
```

---

## Where to go from here

- REST equivalents (with full payload shapes): [`rest-api.md`](./rest-api.md).
- Event consumption (push side of the API): [`events.md`](./events.md).
- Why some calls return `DEADLINE_EXCEEDED` and how to retry safely: [`../guides/use-the-rest-api.md`](../guides/use-the-rest-api.md).
