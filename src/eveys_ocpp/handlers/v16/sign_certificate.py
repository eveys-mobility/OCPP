"""SignCertificate handler (charger-initiated).

OCPP 1.6 reference: Security Whitepaper §4.13 SignCertificate.req /
SignCertificate.conf.

The charger sends a CSR (PEM) asking the CSMS to mint a signed
certificate. This is the **inbound-only** slice (#186):

- Persist the CSR into `pending_certificate_signings` for operator
  review.
- Emit a `cp.csr_submitted` Kafka event so external systems can
  observe pending requests.
- Reply per spec — `Accepted` for a non-empty CSR (the gateway
  accepted the request for processing), `Rejected` for an empty
  CSR (clearly malformed; nothing useful to forward).

The actual signing pipeline — i.e. which CA actually signs the
CSR and how the signed chain is delivered back via
`CertificateSigned.req` — is deferred (#187). Until that lands,
chargers re-submit per spec (no `CertificateSigned` reply ever
arrives), and rows accumulate in the table for operator review.

`Accepted` here is the spec's signal that the CSMS accepted the
request for processing, NOT that the certificate has been signed.
The OCPP `CertificateSigned.req` follow-up RPC is what tells the
charger the actual outcome.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ocpp.v16 import call_result
from ocpp.v16.enums import GenericStatus

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.metrics import record_handler_error, time_handler
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import insert_pending_certificate_signing

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


async def handle(cp: EveysChargePoint, *, csr: str, **_: object) -> call_result.SignCertificate:
    bind_contextvars(cp_id=cp.id, action="SignCertificate", direction="rx")

    with time_handler("SignCertificate"):
        try:
            # Empty CSR is the only thing we reject without a DB
            # round-trip — it's clearly malformed and nothing
            # downstream could make sense of it. Real PEM parsing
            # is the operator queue's job (or the signing CA's).
            if not csr or not csr.strip():
                metrics_registry.SIGN_CERTIFICATE_RECEIVED_TOTAL.labels(outcome="rejected").inc()
                log.info("sign_certificate.rejected_empty_csr")
                return call_result.SignCertificate(status=GenericStatus.rejected)

            async with session_scope(cp.session_factory) as session:
                pending_id = await insert_pending_certificate_signing(session, cp_id=cp.id, csr=csr)

            metrics_registry.SIGN_CERTIFICATE_RECEIVED_TOTAL.labels(outcome="accepted").inc()
            log.info("sign_certificate.accepted", pending_id=pending_id)

            # cp.csr_submitted webhook source. Best-effort publish
            # per the same pattern the other emitters use — broker
            # drop must not crash the OCPP handler. Charger gets
            # `Accepted` either way; downstream consumers can
            # always reconcile from the DB.
            if cp.event_producer is not None:
                envelope = events_pb2.EventEnvelope(
                    event_id=str(uuid.uuid4()),
                    occurred_at=datetime.now(UTC).isoformat(),
                    cp_id=cp.id,
                    schema_version="v1",
                    cp_csr_submitted=events_pb2.CpCsrSubmitted(
                        csr=csr,
                        pending_id=pending_id,
                    ),
                )
                try:
                    await cp.event_producer.publish(
                        topic=cp.settings.kafka_topic_cp_csr_submitted,
                        key=cp.id,
                        value=envelope.SerializeToString(),
                    )
                except Exception as exc:
                    log.warning("sign_certificate.publish_failed", error=str(exc))

            return call_result.SignCertificate(status=GenericStatus.accepted)
        except Exception as exc:
            record_handler_error("SignCertificate", exc)
            raise
