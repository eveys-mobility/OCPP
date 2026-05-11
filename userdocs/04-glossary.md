# Glossary

**Audience.** Anyone meeting an OCPP term for the first time.

**What this answers.** One-line definitions for every OCPP-specific term used elsewhere in this set, alphabetised. Keep nearby while you read.

> OCPP itself stands for **Open Charge Point Protocol**, defined by the **Open Charge Alliance (OCA)**. This page covers the terms that turn up when you actually use it. If a term you need is missing, open an issue against the repo.

---

## A

**`Action`.** The OCPP message name that identifies what a CALL is — for example `BootNotification`, `MeterValues`, `RemoteStartTransaction`. Carried as a string in every wire envelope.

**`Authorize`.** OCPP message a charger sends when a user presents an identification token (RFID card, app QR code, plug-and-charge). Asks the central system "is this `id_tag` allowed to charge?". The central system answers with an `IdTagInfo` containing `status` ∈ `Accepted`, `Blocked`, `Expired`, `Invalid`, `ConcurrentTx`.

**`AvailabilityType`.** Enum on `ChangeAvailability`: `Operative` (in service) or `Inoperative` (out of service for maintenance).

## B

**Basic Auth.** Per-charger username and password supplied in the WebSocket upgrade's `Authorization: Basic` header. The username is the charger's `cp_id`; the password is provisioned by the operator. The gateway checks the password against a bcrypt hash in Postgres.

**`BootNotification`.** First message a charger sends on connecting. Carries vendor, model, firmware version, serial number. The central system replies with `Accepted | Pending | Rejected`, a `currentTime` for clock sync, and a `interval` (how often the charger should heartbeat).

## C

**CALL / CALLRESULT / CALLERROR.** The three OCPP wire envelope types. `[2, "message_id", "Action", {…payload…}]` is a CALL; `[3, "message_id", {…payload…}]` is its CALLRESULT; `[4, "message_id", "errorCode", "description", {…}]` is a CALLERROR. The leading integer is the **message type ID**.

**`CertificateSigned`.** Central-system-initiated message: "here is your signed certificate chain". Used to deliver the chain back to a charger after it sent a `SignCertificate.req` with a CSR. Defined by the OCPP Security Whitepaper.

**`ChangeAvailability`.** Central-system-initiated. Take a connector — or the whole charger when `connector_id = 0` — Operative or Inoperative. Reply can be `Accepted | Rejected | Scheduled` (Scheduled means "I have a session in progress; I'll honour this when the session ends").

**`ChangeConfiguration`.** Set one configuration key on a charger. Reply: `Accepted | Rejected | RebootRequired | NotSupported`.

**Charge Point (CP).** OCPP's term for the physical charger device. Identified by a string `cp_id`.

**Charge Point Identity / `cp_id`.** Unique stable identifier for one charger. Appears in the WebSocket URL path, as the partition key on every Kafka event, and as the path parameter on every REST endpoint that targets a specific charger.

**`ChargingProfile`.** A schedule describing how much current/power a charger may draw over time. Pushed from the central system; resolved on the charger.

**`ClearCache`.** "Wipe your authorization cache." The charger's local cache of recent authorize results.

**`ClearChargingProfile`.** Remove one or more `ChargingProfile`s by id / connector / purpose / stack-level.

**Connector.** A specific socket on a charger. A charger has one or more; they are numbered starting from `1`. `connector_id = 0` refers to the whole charger in some commands.

**Central System / Central Management System / CSMS.** The OCPP term for the platform-side service that chargers connect to. This gateway is one component of a CSMS.

**CSR.** Certificate Signing Request. A blob of base64-PEM the charger sends when it wants the central system to mint a signed certificate (see `SignCertificate`).

## D

**`DataTransfer`.** A vendor extension escape hatch. Either side can send `{vendorId, messageId, data}` to convey something not in the standard OCPP spec.

**`DeleteCertificate`.** Central-system-initiated. Delete a certificate the charger has installed, identified by its SHA-256 hash.

**`DiagnosticsStatusNotification`.** Charger-initiated lifecycle for an uploaded diagnostics dump: `Idle | Uploading | Uploaded | UploadFailed`.

## E

**`ExtendedTriggerMessage`.** Security Whitepaper §4.7. Same shape as `TriggerMessage` but supports the wider set of message types added by the Whitepaper (`LogStatusNotification`, `SignChargePointCertificate`).

## F

**`FirmwareStatusNotification`.** Charger-initiated lifecycle for a firmware update: `Idle | Downloading | Downloaded | DownloadFailed | Installing | Installed | InstallationFailed` plus security-profile extensions.

## G

**`GetCompositeSchedule`.** "What schedule is currently effective on this connector for the next N seconds?" The charger answers with the merged result of every active `ChargingProfile`.

**`GetConfiguration`.** Read one or more configuration keys from a charger. Empty key list means "all".

**`GetDiagnostics`.** Central-system-initiated. "Upload your diagnostics blob to this URL." Followed asynchronously by `DiagnosticsStatusNotification`s as the upload progresses.

**`GetInstalledCertificateIds`.** Security Whitepaper §4.8. "What CA certificates do you have installed right now?" Answer is a list of `hash_data` items.

**`GetLocalListVersion`.** "What version of the local auth list is loaded on you right now?" Used before deciding whether to send a full or differential update.

**`GetLog`.** Security Whitepaper §4.6. "Upload a diagnostics or security log to this URL." Followed by `LogStatusNotification`s.

## H

**`hash_data`.** A 4-tuple identifying an installed certificate: `hashAlgorithm`, `issuerNameHash`, `issuerKeyHash`, `serialNumber`. The OCPP §5.1 form.

**`Heartbeat`.** Charger-initiated keep-alive. Carries no data; reply is the central system's `currentTime` for clock sync.

## I

**`id_tag`.** The user identifier the charger reports when a user presents themselves — RFID card UID, app-generated token, plug-and-charge cert thumbprint. Up to 20 characters in OCPP 1.6.

**`IdTagInfo`.** The response shape for `Authorize`: `{status, expiryDate?, parentIdTag?}`.

**`InstallCertificate`.** Security Whitepaper §4.5. Central-system-initiated. "Install this root certificate so you can verify CSMS signatures."

## L

**Local Auth List.** A local copy of accepted `id_tag`s the charger consults when it can't reach the central system. Synced by `SendLocalList` (full or differential).

**`LogStatusNotification`.** Charger-initiated lifecycle for a `GetLog` upload: `Idle | Uploading | Uploaded | UploadFailed`.

## M

**`MeterValues`.** Charger-initiated periodic samples of meter readings during a session — `Energy.Active.Import.Register`, `Voltage`, `Current.Import`, `Power.Active.Import`, `SoC`, etc. The substance of every charging session for billing and dashboards.

**Measurand.** What a `MeterValues` sample measures. Around 25 standard measurands; the ones you'll see most are `Energy.Active.Import.Register` (cumulative Wh), `Voltage`, `Current.Import`, `Power.Active.Import`, `SoC` (state of charge, %).

**mTLS.** Mutual TLS — both endpoints present a certificate to authenticate the other. At the charger ↔ Envoy boundary this means the charger has its own client cert; at the Envoy ↔ gateway boundary it means Envoy and gateway authenticate each other.

## O

**OCPP.** Open Charge Point Protocol. The standard.

**OCPP-J.** JSON-over-WebSocket variant of OCPP. The other variant, OCPP-S (SOAP), is legacy and not supported here.

**OCPP 1.6 / 2.0.1.** Two protocol versions in current deployment. 1.6 is the wide-deploy incumbent; 2.0.1 adds security primitives and a richer device-model.

**OCA / Open Charge Alliance.** The standards body that defines OCPP. Owns the testing tool used for formal conformance.

**OCTT.** Open Charge Test Tool. The OCA-published conformance suite. Required for an officially certified implementation; access is gated on OCA membership.

## R

**`RemoteStartTransaction`.** Central-system-initiated. "Start a charging session for this `id_tag` on this `connector_id` (optionally with this `ChargingProfile`)." Reply `Accepted | Rejected`. **Accepted means "I will try"**, not "the session has started" — the actual session start is signalled by a subsequent `StartTransaction.req` from the charger.

**`RemoteStopTransaction`.** Central-system-initiated. "End this transaction by ID." Reply `Accepted | Rejected`.

**Replay.** OCPP allows chargers to re-send messages they think didn't land. The gateway dedupes these — see [`concepts/idempotency-and-replay.md`](./concepts/idempotency-and-replay.md).

**`ReserveNow` / `CancelReservation`.** Central-system-initiated. Hold a connector for a specific `id_tag` until an expiry time.

**`Reset`.** Central-system-initiated. `Soft` reboots the controller; `Hard` cycles power.

## S

**`SecurityEventNotification`.** Charger-initiated audit event from the Security Whitepaper: `InvalidSignature`, `InvalidCertificate`, `InvalidCsmsCertificate`, `TamperDetectionActivated`, …

**`SendLocalList`.** Central-system-initiated. Push the Local Auth List (full or differential) to a charger.

**`SetChargingProfile`.** Central-system-initiated. Install a `ChargingProfile` on a charger.

**`SignCertificate`.** Security Whitepaper §4.13. Charger-initiated. "Please sign this CSR for me." Reply `Accepted | Rejected`. The signed chain is delivered later via `CertificateSigned`.

**`SignedFirmwareStatusNotification`.** Like `FirmwareStatusNotification` but for `SignedUpdateFirmware`; adds `SignatureVerified`, `InvalidSignature`, etc. to the status set.

**`SignedUpdateFirmware`.** Security Whitepaper §4.4. Central-system-initiated firmware update with a cryptographic signature.

**SoC.** State of charge — battery percentage. Reported in `MeterValues` when the EV supplies it.

**`StartTransaction`.** Charger-initiated. "I just started session N for `id_tag` on connector C at meter reading M." Reply assigns a `transactionId`. This is the canonical session-start signal; `RemoteStart`'s reply is *not*.

**`StatusNotification`.** Charger-initiated. Reports a connector's state machine transition: `Available | Preparing | Charging | SuspendedEVSE | SuspendedEV | Finishing | Reserved | Unavailable | Faulted`, plus an OCPP-defined `errorCode`.

**`StopTransaction`.** Charger-initiated. "Session N ended at meter reading M, with reason R." Carries the final `MeterValues` for that session.

**Subprotocol.** The WebSocket subprotocol negotiated on the upgrade handshake. `ocpp1.6` or `ocpp2.0.1`. Without a matching negotiation, the WebSocket is rejected.

## T

**Transaction.** One charging session, from `StartTransaction` to `StopTransaction`. Identified by `transactionId` (assigned by the central system in the `StartTransaction.conf`).

**`TriggerMessage`.** Central-system-initiated. "Please send me a fresh `BootNotification` / `StatusNotification` / `MeterValues` / `Heartbeat` / `DiagnosticsStatusNotification` / `FirmwareStatusNotification`." Reply `Accepted | Rejected | NotImplemented`. See also `ExtendedTriggerMessage`.

## U

**`UnlockConnector`.** Central-system-initiated. Mechanically unlock the cable on `connector_id`. Reply `Unlocked | UnlockFailed | NotSupported`.

**`UpdateFirmware`.** Central-system-initiated firmware update without signature verification (legacy; superseded by `SignedUpdateFirmware` in the Security Whitepaper).

---

## Where to go from here

- Want to see these terms in their natural habitat? [`concepts/how-ocpp-flows-work.md`](./concepts/how-ocpp-flows-work.md) walks a full session.
- Looking up a REST endpoint? [`reference/rest-api.md`](./reference/rest-api.md).
- Looking up an event? [`reference/events.md`](./reference/events.md).
