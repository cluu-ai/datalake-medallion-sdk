"""Recipe: an AWS Lambda that ingests Kinesis, SQS or CloudWatch Logs batches into Bronze.

Not part of the SDK API: copy this file next to your handler and adapt it.
It only uses public SDK calls (``ingest_otel_log_batch``, ``ingest`` and
``batch_id_for``), which give the guarantees that matter:

* **One write per tenant per batch**, not one object per message.
* **Idempotent redelivery**: the batch id comes from the message ids, so a
  batch delivered again (a timeout, a crash) rewrites the same objects.
* **Partial failures**: only the messages that carried a failed tenant's
  records are reported in ``batchItemFailures`` (enable
  ``ReportBatchItemFailures`` on the event source mapping).

Handler::

    from datalake_medallion_sdk import BronzeIngestionClient, ContractRegistry, TenantRegistry
    from datalake_medallion_sdk.otel import logs_contract
    from aws_lambda_handler import LambdaIngestor

    client = BronzeIngestionClient(                 # once per container
        ContractRegistry.from_schemas([logs_contract()]),
        tenants=TenantRegistry.from_config(...),
    )
    ingest = LambdaIngestor(client, contract="otel/logs/1.0.0", payload="otlp")

    def handler(event, context):
        return ingest(event)

Payloads: ``"otlp"`` (each message an OTLP/JSON logs request) or ``"json"``
(each message a JSON object, an array of objects or JSON Lines, with the tenant
in ``tenant_field``, removed from the record because the tenant is stored in
``_dl_tenant_id``, or fixed by ``tenant``). CloudWatch Logs subscription data
(direct, or gzip-compressed through Kinesis) is detected and mapped to the
OpenTelemetry logs contract.

Delivery is at-least-once when only some tenants fail: Kinesis retries from
the first failed record, so later records of that batch may be written again
under a new batch id (deduplicate in Silver). Messages that can never be read
are reported too: configure an on-failure destination (SQS redrive policy,
Kinesis on-failure destination) so they are set aside after a few attempts.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from datalake_medallion_sdk import (
    BronzeIngestionClient,
    ConfigurationError,
    IngestReport,
    TenantProfile,
    TenantViolationError,
    batch_id_for,
)

_log = logging.getLogger(__name__)
_GZIP = b"\x1f\x8b"


@dataclass(frozen=True, slots=True)
class Message:
    """One message of the event batch."""

    id: str  # Kinesis sequence number or SQS message id (the itemIdentifier)
    data: bytes
    arrival_ms: int


@dataclass(frozen=True, slots=True)
class LambdaResult:
    batch_id: str
    reports: dict[str, IngestReport]
    failed: list[str]

    def response(self) -> dict[str, Any]:
        """Partial-batch response for Kinesis and SQS."""
        return {"batchItemFailures": [{"itemIdentifier": i} for i in self.failed]}


def _decoded(data: bytes) -> bytes:
    return gzip.decompress(data) if data[:2] == _GZIP else data


def messages(event: Mapping[str, Any]) -> tuple[str, list[Message]]:
    """The messages of a Kinesis, SQS or CloudWatch Logs event."""
    if "awslogs" in event:
        data = _decoded(base64.b64decode(event["awslogs"]["data"]))
        return "cloudwatch", [Message("awslogs", data, int(time.time() * 1000))]
    records = event.get("Records")
    if not isinstance(records, list):
        raise ConfigurationError("not a Kinesis, SQS or CloudWatch Logs event")
    out: list[Message] = []
    sources = set()
    for r in records:
        source = r.get("eventSource")
        sources.add(source)
        if source == "aws:kinesis":
            k = r["kinesis"]
            arrival = int(float(k.get("approximateArrivalTimestamp", time.time())) * 1000)
            out.append(Message(k["sequenceNumber"], _decoded(base64.b64decode(k["data"])), arrival))
        elif source == "aws:sqs":
            sent = r.get("attributes", {}).get("SentTimestamp")
            arrival = int(sent) if sent else int(time.time() * 1000)
            out.append(Message(r["messageId"], _decoded(r["body"].encode()), arrival))
        else:
            raise ConfigurationError(f"unsupported event source {source!r}")
    if len(sources) > 1:
        raise ConfigurationError("mixed event sources in one batch")
    return (sources.pop() if sources else "aws:sqs"), out


def is_cloudwatch(data: bytes) -> bool:
    head = data[:4096]
    return data[:1] == b"{" and b'"logEvents"' in head and b'"messageType"' in head


def cloudwatch_to_otlp(
    data: bytes, tenant_attribute: str, tenant_of: Callable[[str], str | None] | None
) -> dict[str, Any] | None:
    """A CloudWatch Logs subscription payload as an OTLP/JSON logs request
    (``None`` for control messages)."""
    payload = json.loads(data)
    if payload.get("messageType") != "DATA_MESSAGE":
        return None
    group, stream = payload.get("logGroup", ""), payload.get("logStream", "")

    def attr(key: str, value: str) -> dict[str, Any]:
        return {"key": key, "value": {"stringValue": value}}

    resource = [
        attr("cloud.provider", "aws"),
        attr("service.name", group),
        attr("aws.log.group.names", group),
        attr("aws.log.stream.names", stream),
    ]
    tenant = tenant_of(group) if tenant_of else None
    if tenant:
        resource.append(attr(tenant_attribute, tenant))
    records = [
        {
            "timeUnixNano": str(int(e["timestamp"]) * 1_000_000),
            "body": {"stringValue": e.get("message", "")},
            "attributes": [attr("aws.log.event.id", str(e.get("id", "")))],
        }
        for e in payload.get("logEvents", [])
    ]
    return {
        "resourceLogs": [
            {"resource": {"attributes": resource}, "scopeLogs": [{"logRecords": records}]}
        ]
    }


def json_records(data: bytes) -> list[Any]:
    text = data.strip()
    if text[:1] == b"[":
        loaded = json.loads(text)
        return loaded if isinstance(loaded, list) else [loaded]
    try:
        return [json.loads(text)]
    except json.JSONDecodeError:
        return [json.loads(line) for line in text.splitlines() if line.strip()]


class LambdaIngestor:
    """``ingest(event)`` returns the Lambda response; see the module docs."""

    def __init__(
        self,
        client: BronzeIngestionClient,
        *,
        contract: str,
        payload: Literal["otlp", "json"] = "otlp",
        tenant: TenantProfile | str | None = None,
        tenant_attribute: str = "tenant.id",
        tenant_field: str | None = None,
        cloudwatch_tenant: Callable[[str], str | None] | None = None,
    ) -> None:
        if payload == "json" and tenant is None and tenant_field is None:
            raise ConfigurationError("payload='json' needs tenant= or tenant_field=")
        self.client = client
        self.contract = contract
        self.payload = payload
        self.tenant = tenant
        self.tenant_attribute = tenant_attribute
        self.tenant_field = tenant_field
        self.cloudwatch_tenant = cloudwatch_tenant

    def __call__(self, event: Mapping[str, Any], context: Any = None) -> dict[str, Any]:
        return asyncio.run(self.ingest(event)).response()

    async def ingest(self, event: Mapping[str, Any]) -> LambdaResult:
        source, batch = messages(event)
        if not batch:
            return LambdaResult("", {}, [])
        batch_id = batch_id_for([m.id for m in batch], time_ms=max(m.arrival_ms for m in batch))
        failed: set[int] = set()
        otlp: list[bytes] = []
        owners: list[int] = []  # message of each OTLP request
        groups: dict[str, tuple[list[Any], set[int]]] = {}
        for index, message in enumerate(batch):
            try:
                if source == "cloudwatch" or is_cloudwatch(message.data):
                    request = cloudwatch_to_otlp(
                        message.data, self.tenant_attribute, self.cloudwatch_tenant
                    )
                    if request is not None:
                        otlp.append(json.dumps(request, separators=(",", ":")).encode())
                        owners.append(index)
                elif self.payload == "otlp":
                    otlp.append(message.data)
                    owners.append(index)
                else:
                    for record in json_records(message.data):
                        tenant = self._tenant_of(record)
                        if self.tenant_field is not None and isinstance(record, dict):
                            # The tenant is stored in _dl_tenant_id; the
                            # routing field is not part of the contract.
                            record = {k: v for k, v in record.items() if k != self.tenant_field}
                        records, sources = groups.setdefault(tenant, ([], set()))
                        records.append(record)
                        sources.add(index)
            except (ValueError, KeyError, TypeError, TenantViolationError) as e:
                _log.warning("message %s cannot be ingested: %s", message.id, type(e).__name__)
                failed.add(index)
        reports: dict[str, IngestReport] = {}
        if otlp:
            outcomes = await self.client.ingest_otel_log_batch(
                otlp,
                contract=self.contract,
                tenant_attribute=self.tenant_attribute,
                default_tenant=self.tenant,
                batch_id=batch_id,
            )
            for o in outcomes:
                if o.report is not None:
                    reports[o.report.tenant_id] = o.report
                else:
                    _log.error("tenant %s of batch %s failed: %r", o.tenant_id, batch_id, o.error)
                    failed.update(owners[i] for i in o.requests)
        if groups:
            tenants = list(groups)
            results = await asyncio.gather(
                *(
                    self.client.ingest(
                        json.dumps(groups[t][0], separators=(",", ":")).encode(),
                        tenant=t,
                        contract=self.contract,
                        batch_id=batch_id,
                    )
                    for t in tenants
                ),
                return_exceptions=True,
            )
            for tenant, result in zip(tenants, results, strict=True):
                if isinstance(result, BaseException):
                    _log.error("tenant %s of batch %s failed: %r", tenant, batch_id, result)
                    failed.update(groups[tenant][1])
                else:
                    reports[result.tenant_id] = result
        return LambdaResult(batch_id, reports, [batch[i].id for i in sorted(failed)])

    def _tenant_of(self, record: Any) -> str:
        if self.tenant_field is not None and isinstance(record, dict):
            value = record.get(self.tenant_field)
            if isinstance(value, str) and value:
                return value
        if isinstance(self.tenant, TenantProfile):
            return self.tenant.tenant_id
        if isinstance(self.tenant, str):
            return self.tenant
        raise TenantViolationError(f"record without {self.tenant_field!r} and no default tenant")
