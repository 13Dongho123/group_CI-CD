#!/usr/bin/env python3
"""Group batch sender -> BatchLoaderLambda (non-Kinesis)."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

LOG = logging.getLogger("group_sender")


def _setup_logging() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _source() -> str:
    v = os.environ.get("SOURCE_NAME", "group").strip()
    return v or "group"


def _record_count() -> int:
    try:
        n = int(os.environ.get("GROUP_BATCH_RECORDS", "100"))
    except ValueError:
        n = 100
    return max(1, min(n, 5000))


def _interval_sec() -> int:
    try:
        n = int(os.environ.get("GROUP_BATCH_INTERVAL_SEC", "120"))
    except ValueError:
        n = 120
    return max(10, n)


def _batch_payload(source_name: str) -> dict:
    now = _utc_now_iso()
    rows = []
    for i in range(_record_count()):
        rows.append(
            {
                "customer_id": f"{source_name[:3]}-{random.randint(100000, 999999)}",
                "amount": random.randint(1000, 500000),
                "currency": "KRW",
                "event_index": i + 1,
                "timestamp": now,
            }
        )
    file_name = f"{source_name}_{_today().replace('-', '')}.json"
    batch_id = hashlib.sha1(f"{source_name}:{now}:{len(rows)}".encode("utf-8")).hexdigest()[:16]
    return {
        "source_name": source_name,
        "timestamp": now,
        "file_name": file_name,
        "batch_id": batch_id,
        "record_count": len(rows),
        "records": rows,
    }


def _persist_local(payload: dict) -> None:
    out_dir = Path(os.environ.get("GROUP_LOCAL_BATCH_DIR", "/opt/group-agent/batches"))
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / str(payload["file_name"])
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    LOG.info("Local batch file written: %s", p)


def _invoke_lambda(payload: dict) -> None:
    fn = os.environ.get("BATCH_LOADER_LAMBDA_NAME", "").strip()
    if not fn:
        raise RuntimeError("BATCH_LOADER_LAMBDA_NAME is not set")
    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "ap-northeast-2"
    client = boto3.client("lambda", region_name=region)
    event = {"Records": [{"body": json.dumps(payload, ensure_ascii=False)}]}
    resp = client.invoke(
        FunctionName=fn,
        InvocationType="RequestResponse",
        Payload=json.dumps(event).encode("utf-8"),
    )
    status = int(resp.get("StatusCode", 0))
    body = (resp.get("Payload").read() if resp.get("Payload") else b"").decode("utf-8", errors="replace")
    if status < 200 or status >= 300:
        raise RuntimeError(f"lambda invoke failed: status={status} body={body[:300]}")
    LOG.info("Lambda invoke OK: function=%s status=%s body=%s", fn, status, body[:240])


def run_sender(source_default: str | None = None) -> int:
    _setup_logging()
    source_name = source_default or _source()
    interval = _interval_sec()
    LOG.info("Starting group batch sender: source=%s interval=%ss", source_name, interval)
    while True:
        send_once(source_name)
        time.sleep(interval)


def send_once(source_name: str) -> int:
    payload = _batch_payload(source_name)
    _persist_local(payload)
    try:
        _invoke_lambda(payload)
        return 0
    except (BotoCoreError, ClientError, RuntimeError) as e:
        LOG.error("Batch send failed: %s", e)
        return 1


def main() -> int:
    return run_sender()


if __name__ == "__main__":
    sys.exit(main())
