"""Recipe: an AWS Lambda that ingests Kinesis, SQS, S3 or CloudWatch Logs batches into Bronze.

Not part of the SDK API: copy this file next to your handler and adapt it.
It only uses public SDK calls (``ingest_otel_log_batch``, ``ingest`` and
``batch_id_for``), which give the guarantees that matter:

* **One write per tenant per batch**, not one object per message (per S3
  object for S3 notifications).
* **Idempotent redelivery**: the batch id comes from the message ids, so a
  batch delivered again (a timeout, a crash) rewrites the same objects. Each S3
  object is a batch of its own named by its key and ETag, so it is rewritten in
  place however SQS groups its notifications.
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

Payloads: ``"otlp"`` (each message an OTLP logs request) or ``"json"`` (each
message a JSON object, an array of objects or JSON Lines, with the tenant in
``tenant_field``, removed from the record because the tenant is stored in
``_dl_tenant_id``, or fixed by ``tenant``). OTLP requests may be protobuf or
JSON, gzip or zstd compressed; the SDK detects them:

* **Kinesis**: the record data as written (raw protobuf, JSON or compressed).
* **SQS**: JSON text, or binary requests in base64. A ``content-type``
  message attribute (``application/x-protobuf`` or ``application/json``)
  says which; without it, a body that does not start with ``{`` is decoded
  from base64.
* **S3 through SQS**: ``ObjectCreated`` notifications of a landing bucket
  (for example where the Collector's ``awss3exporter`` writes one request per
  object, ``marshaler: otlp_proto``). Each object is downloaded and ingested
  as a batch of its own, with the batch id from its key and ETag.

CloudWatch Logs subscription data (direct, or gzip-compressed through
Kinesis) is detected and mapped to the OpenTelemetry logs contract.

Delivery is at-least-once when only some tenants fail: Kinesis retries from
the first failed record, so later records of that batch may be written again
under a new batch id (deduplicate in Silver). Messages that can never be read
are reported too: configure an on-failure destination (SQS redrive policy,
Kinesis on-failure destination) so they are set aside after a few attempts.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import time
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from urllib.parse import unquote_plus

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
_MIB = 1024 * 1024
_PROTOBUF_TYPES = ("application/x-protobuf", "application/protobuf")


@dataclass(frozen=True, slots=True)
class Message:
    """One message of the event batch."""

    id: str  # Kinesis sequence number or SQS message id (the itemIdentifier)
    data: bytes  # as delivered: possibly compressed
    arrival_ms: int
    objects: list[tuple[str, str, str, int]] = field(default_factory=list)
    """S3 objects the message announces: (bucket, key, etag, size)."""
    notification: bool = False
    """An S3 notification: its content is in ``objects`` (none for test events)."""


@dataclass(frozen=True, slots=True)
class LambdaResult:
    batch_id: str
    """Batch id of the Kinesis or SQS messages (S3 objects have their own)."""
    reports: list[IngestReport]
    """One per tenant and batch (a tenant may appear in several S3 objects)."""
    failed: list[str]

    def response(self) -> dict[str, Any]:
        """Partial-batch response for Kinesis and SQS."""
        return {"batchItemFailures": [{"itemIdentifier": i} for i in self.failed]}


def _inflate(data: bytes, limit: int) -> bytes:
    """gzip data decompressed, at most ``limit`` bytes; other data unchanged."""
    if data[:2] != _GZIP:
        return data
    inflater = zlib.decompressobj(wbits=31)
    out = inflater.decompress(data, limit)
    if inflater.unconsumed_tail:
        raise ValueError(f"message expands beyond {limit} bytes")
    return out


def _head(data: bytes, size: int = 4096) -> bytes:
    """The first bytes of the message, decompressing only what they need."""
    if data[:2] != _GZIP:
        return data[:size]
    try:
        return zlib.decompressobj(wbits=31).decompress(data, size)
    except zlib.error:
        return b""


def _event_ms(value: str | None) -> int:
    if not value:
        return int(time.time() * 1000)
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _s3_notice(body: str) -> tuple[list[tuple[str, str, str, int]], int | None] | None:
    """The created objects of an S3 notification and its latest event time
    (no objects for a test event or other events), or ``None`` if the body is
    not one."""
    head = body[:4096]
    if not body.startswith("{") or ('"aws:s3"' not in head and "s3:TestEvent" not in head):
        return None
    try:
        notice = json.loads(body)
    except json.JSONDecodeError:
        return None
    if notice.get("Event") == "s3:TestEvent":
        return [], None
    records = notice.get("Records")
    if not isinstance(records, list) or not records:
        return None
    if not all(isinstance(r, dict) and r.get("eventSource") == "aws:s3" for r in records):
        return None
    created = [r for r in records if str(r.get("eventName", "")).startswith("ObjectCreated")]
    objects = [
        (
            r["s3"]["bucket"]["name"],
            unquote_plus(r["s3"]["object"]["key"]),
            r["s3"]["object"].get("eTag", ""),
            int(r["s3"]["object"].get("size", 0)),
        )
        for r in created
    ]
    return objects, max((_event_ms(r.get("eventTime")) for r in created), default=None)


def _sqs_data(record: Mapping[str, Any]) -> bytes:
    """The SQS body as bytes: text as is, binary requests decoded from base64."""
    body: str = record["body"]
    attributes = {k.lower(): v for k, v in record.get("messageAttributes", {}).items()}
    content_type = str(attributes.get("content-type", {}).get("stringValue", "")).lower()
    if content_type.startswith(_PROTOBUF_TYPES):
        return base64.b64decode(body, validate=True)
    if content_type or body.lstrip().startswith("{"):
        return body.encode()
    try:
        return base64.b64decode(body, validate=True)
    except binascii.Error:
        return body.encode()


def messages(event: Mapping[str, Any]) -> tuple[str, list[Message]]:
    """The messages of a Kinesis, SQS or CloudWatch Logs event."""
    if "awslogs" in event:
        data = base64.b64decode(event["awslogs"]["data"])
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
            out.append(Message(k["sequenceNumber"], base64.b64decode(k["data"]), arrival))
        elif source == "aws:sqs":
            sent = r.get("attributes", {}).get("SentTimestamp")
            arrival = int(sent) if sent else int(time.time() * 1000)
            notice = _s3_notice(r["body"])
            if notice is not None:
                objects, event_ms = notice
                out.append(Message(r["messageId"], b"", event_ms or arrival, objects, True))
            else:
                out.append(Message(r["messageId"], _sqs_data(r), arrival))
        else:
            raise ConfigurationError(f"unsupported event source {source!r}")
    if len(sources) > 1:
        raise ConfigurationError("mixed event sources in one batch")
    return (sources.pop() if sources else "aws:sqs"), out


def is_cloudwatch(data: bytes) -> bool:
    head = _head(data)
    return head[:1] == b"{" and b'"logEvents"' in head and b'"messageType"' in head


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
    """``ingest(event)`` returns the Lambda response; see the module docs.

    ``s3`` is the boto3 S3 client used for S3 notifications (created on first
    use when not given). ``max_object_mb`` bounds an S3 object and a
    decompressed message.
    """

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
        s3: Any = None,
        max_object_mb: int = 64,
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
        self.max_object_mb = max_object_mb
        self._s3 = s3

    def __call__(self, event: Mapping[str, Any], context: Any = None) -> dict[str, Any]:
        return asyncio.run(self.ingest(event)).response()

    @property
    def s3(self) -> Any:
        if self._s3 is None:
            import boto3

            self._s3 = boto3.client("s3")
        return self._s3

    def _download(self, bucket: str, key: str, _etag: str, size: int) -> bytes:
        if size > self.max_object_mb * _MIB:
            raise ValueError(f"object is larger than {self.max_object_mb} MiB")
        body: bytes = self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        return body

    async def _objects(
        self, batch: list[Message], failed: set[int]
    ) -> list[tuple[int, str, bytes]]:
        """The S3 objects announced in the batch: (message index, object id, content)."""
        wanted = [(i, o) for i, m in enumerate(batch) for o in m.objects]
        results = await asyncio.gather(
            *(asyncio.to_thread(self._download, *o) for _, o in wanted), return_exceptions=True
        )
        out: list[tuple[int, str, bytes]] = []
        for (index, (bucket, key, etag, _)), result in zip(wanted, results, strict=True):
            if isinstance(result, BaseException):
                _log.warning("s3://%s/%s cannot be read: %s", bucket, key, type(result).__name__)
                failed.add(index)
            else:
                out.append((index, f"s3://{bucket}/{key}@{etag}", result))
        return out

    async def ingest(self, event: Mapping[str, Any]) -> LambdaResult:
        source, batch = messages(event)
        failed: set[int] = set()
        direct = [(i, m.data) for i, m in enumerate(batch) if not m.notification]
        batch_id = ""
        jobs: list[tuple[str, list[tuple[int, bytes]]]] = []  # (batch id, [(message, content)])
        if direct:
            batch_id = batch_id_for(
                [batch[i].id for i, _ in direct],
                time_ms=max(batch[i].arrival_ms for i, _ in direct),
            )
            jobs.append((batch_id, direct))
        # Each S3 object is a batch of its own, named by its key and ETag: the
        # same object announced again rewrites the same Bronze objects, however
        # SQS groups the notifications.
        for index, object_id, data in await self._objects(batch, failed):
            object_batch = batch_id_for([object_id], time_ms=batch[index].arrival_ms)
            jobs.append((object_batch, [(index, data)]))
        results = await asyncio.gather(
            *(self._ingest(source, batch, job_id, contents, failed) for job_id, contents in jobs)
        )
        reports = [r for job in results for r in job]
        return LambdaResult(batch_id, reports, [batch[i].id for i in sorted(failed)])

    async def _ingest(
        self,
        source: str,
        batch: list[Message],
        batch_id: str,
        contents: list[tuple[int, bytes]],
        failed: set[int],
    ) -> list[IngestReport]:
        """Ingests the contents of some messages as one batch; adds failures to ``failed``."""
        limit = self.max_object_mb * _MIB
        otlp: list[bytes] = []
        owners: list[int] = []  # message of each OTLP request
        groups: dict[str, tuple[list[Any], set[int]]] = {}
        for index, data in contents:
            try:
                if source == "cloudwatch" or is_cloudwatch(data):
                    request = cloudwatch_to_otlp(
                        _inflate(data, limit), self.tenant_attribute, self.cloudwatch_tenant
                    )
                    if request is not None:
                        otlp.append(json.dumps(request, separators=(",", ":")).encode())
                        owners.append(index)
                elif self.payload == "otlp":
                    # Any encoding and compression: the SDK detects them.
                    otlp.append(data)
                    owners.append(index)
                else:
                    for record in json_records(_inflate(data, limit)):
                        tenant = self._tenant_of(record)
                        if self.tenant_field is not None and isinstance(record, dict):
                            # The tenant is stored in _dl_tenant_id; the
                            # routing field is not part of the contract.
                            record = {k: v for k, v in record.items() if k != self.tenant_field}
                        records, sources = groups.setdefault(tenant, ([], set()))
                        records.append(record)
                        sources.add(index)
            except (ValueError, KeyError, TypeError, zlib.error, TenantViolationError) as e:
                _log.warning("message %s cannot be ingested: %s", batch[index].id, type(e).__name__)
                failed.add(index)
        reports: list[IngestReport] = []
        if otlp:
            outcomes = await self.client.ingest_otel_log_batch(
                otlp,
                contract=self.contract,
                tenant_attribute=self.tenant_attribute,
                default_tenant=self.tenant,
                batch_id=batch_id,
                max_decompressed_mb=self.max_object_mb,
            )
            for o in outcomes:
                if o.report is not None:
                    reports.append(o.report)
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
                    reports.append(result)
        return reports

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
