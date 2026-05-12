#!/usr/bin/env python3
"""Group batch sender -> BatchLoaderLambda (non-Kinesis).

운영 모델 (예: 2분마다 cron):

- 직전 배치가 **성공적으로** S3까지 간 시점을 워터마크(`GROUP_STATE_DIR/watermark_<source>.json`)에 저장.
- 다음 실행에서 DB는 **(워터마크 시각, 이번 실행의 기준 시각]** 구간만 SELECT 해서 JSON으로 묶어 Lambda → S3.
- cron 주기를 `GROUP_BATCH_INTERVAL_SEC`(예: 600)와 맞추면, 정상 동작 시 **“그 사이(약 10분) 동안 테이블에 쌓인 행”**이 한 번에 나가는 것과 같다
  (지연·재시도가 있으면 구간이 길어질 수 있고, 그때는 같은 규칙으로 뒤늦게 한꺼번에 나간다).

한 틱 안에서 (선택) `GROUP_INSERT_PER_TICK`만큼 샘플 INSERT 후 곧바로 위 SELECT를 수행한다.
적재를 cron 밖에서만 하려면 `GROUP_INSERT_PER_TICK=0`.

실행 방식:

- **systemd 루프**: `active_sender.py` → `run_sender` → `sleep(GROUP_BATCH_INTERVAL_SEC)` 반복 (설정 초마다 한 사이클).
- **cron 한 방**: `GROUP_RUN_ONCE=1` 두고 동일 바이너리로 한 번만 `send_once` (또는 `batch_export.py`).

MySQL 미설정 시: 레거시 메모리 난수 배치. `GROUP_USE_MYSQL_EXPORT=false`로 강제 가능.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import secrets
import sys
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import boto3
from botocore.exceptions import BotoCoreError, ClientError

LOG = logging.getLogger("group_sender")

try:
    import pymysql
    from pymysql.cursors import DictCursor
except ImportError:
    pymysql = None  # type: ignore[assignment]
    DictCursor = None  # type: ignore[assignment]

InsertFn = Callable[[Any, int], None]
FetchFn = Callable[[Any, datetime, datetime, int], list[dict[str, Any]]]

# (insert_rows, fetch_rows, time_column_name for watermark advancement)
_SOURCE_REGISTRY: dict[str, tuple[InsertFn, FetchFn, str]] = {}


def _bank_ops() -> tuple[InsertFn, FetchFn]:
    def insert_rows(cur: Any, n: int) -> None:
        for _ in range(n):
            bid = f"b{secrets.token_hex(8)}"[:30]
            uid = f"LS{random.randint(100000, 999999)}"
            cur.execute(
                "INSERT IGNORE INTO bank_customer (bank_id, ls_user_id, account_no, account_type, branch_code, join_dt) "
                "VALUES (%s,%s,%s,%s,%s,CURDATE())",
                (bid, uid, f"ACC{uid}", "SAV", "BR01"),
            )
            cur.execute(
                "INSERT INTO bank_transaction (bank_id, ls_user_id, transaction_id, transaction_dt, transaction_type, "
                "amount, balance_after, merchant_category, channel) "
                "VALUES (%s,%s,%s,NOW(),%s,%s,%s,%s,%s)",
                (
                    bid,
                    uid,
                    f"TXN{random.randint(10**6, 10**9)}",
                    random.choice(("DPST", "WDTH")),
                    random.randint(1, 50000),
                    random.randint(1, 200000),
                    "FOOD",
                    random.choice(("MOBILE", "WEB")),
                ),
            )

    def fetch_rows(cur: Any, since: datetime, until: datetime, limit: int) -> list[dict[str, Any]]:
        cur.execute(
            "SELECT txn_id, bank_id, ls_user_id, transaction_id, transaction_dt, transaction_type, amount, "
            "balance_after, merchant_category, channel FROM bank_transaction "
            "WHERE transaction_dt > %s AND transaction_dt <= %s ORDER BY transaction_dt ASC LIMIT %s",
            (since, until, limit),
        )
        return list(cur.fetchall())

    return insert_rows, fetch_rows


def _card_ops() -> tuple[InsertFn, FetchFn]:
    def insert_rows(cur: Any, n: int) -> None:
        for _ in range(n):
            cid = f"c{secrets.token_hex(8)}"[:30]
            uid = f"LS{random.randint(100000, 999999)}"
            cur.execute(
                "INSERT IGNORE INTO card_customer (card_id, ls_user_id, card_grade, limit_amount, issue_dt) "
                "VALUES (%s,%s,%s,%s,CURDATE())",
                (cid, uid, "GOLD", random.randint(100000, 9000000)),
            )
            cur.execute(
                "INSERT INTO card_approval (card_id, ls_user_id, approval_no, approval_dt, merchant_name, "
                "merchant_category, amount, installment_months, payment_status) "
                "VALUES (%s,%s,%s,NOW(),%s,%s,%s,%s,%s)",
                (
                    cid,
                    uid,
                    f"APR{random.randint(10**6, 10**9)}",
                    f"Merchant{random.randint(1000, 9999)}",
                    "RETAIL",
                    random.randint(1, 200000),
                    0,
                    "APPROVED",
                ),
            )

    def fetch_rows(cur: Any, since: datetime, until: datetime, limit: int) -> list[dict[str, Any]]:
        cur.execute(
            "SELECT id, card_id, ls_user_id, approval_no, approval_dt, merchant_name, merchant_category, amount, "
            "installment_months, payment_status FROM card_approval "
            "WHERE approval_dt > %s AND approval_dt <= %s ORDER BY approval_dt ASC LIMIT %s",
            (since, until, limit),
        )
        return list(cur.fetchall())

    return insert_rows, fetch_rows


def _sec_ops() -> tuple[InsertFn, FetchFn]:
    def insert_rows(cur: Any, n: int) -> None:
        for _ in range(n):
            sid = f"s{secrets.token_hex(8)}"[:30]
            uid = f"LS{random.randint(100000, 999999)}"
            cur.execute(
                "INSERT IGNORE INTO securities_account (sec_id, ls_user_id, risk_grade, open_dt) "
                "VALUES (%s,%s,%s,CURDATE())",
                (sid, uid, "MID"),
            )
            cur.execute(
                "INSERT INTO securities_trade (sec_id, trade_id, trade_dt, stock_code, asset_type, trade_type, "
                "quantity, trade_price, trade_amount, return_rate) "
                "VALUES (%s,%s,NOW(),%s,%s,%s,%s,%s,%s,%s)",
                (
                    sid,
                    f"TR{random.randint(10**6, 10**9)}",
                    "005930",
                    "STOCK",
                    "BUY",
                    random.randint(1, 100),
                    random.randint(1000, 100000),
                    random.randint(10000, 5000000),
                    Decimal("0.0123"),
                ),
            )

    def fetch_rows(cur: Any, since: datetime, until: datetime, limit: int) -> list[dict[str, Any]]:
        cur.execute(
            "SELECT id, sec_id, trade_id, trade_dt, stock_code, asset_type, trade_type, quantity, trade_price, "
            "trade_amount, return_rate FROM securities_trade "
            "WHERE trade_dt > %s AND trade_dt <= %s ORDER BY trade_dt ASC LIMIT %s",
            (since, until, limit),
        )
        return list(cur.fetchall())

    return insert_rows, fetch_rows


def _insurance_like_insert(cur: Any, n: int) -> None:
    for _ in range(n):
        pol = f"POL{secrets.token_hex(8)}"
        uid = f"LS{random.randint(100000, 999999)}"
        cur.execute(
            "INSERT IGNORE INTO insurance_policy (policy_no, ins_id, ls_user_id, product_code, product_name, "
            "premium_amount, `status`) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (
                pol,
                f"INS{uid}",
                uid,
                f"P{random.randint(1000, 9999)}",
                "Product",
                random.randint(10000, 500000),
                "ACTIVE",
            ),
        )
        cur.execute(
            "INSERT INTO insurance_event (policy_no, event_type, event_dt, payment_status, amount) "
            "VALUES (%s,%s,NOW(),%s,%s)",
            (pol, "PAYMENT", "OK", random.randint(1000, 100000)),
        )


def _insurance_like_fetch(cur: Any, since: datetime, until: datetime, limit: int) -> list[dict[str, Any]]:
    cur.execute(
        "SELECT id, policy_no, event_type, event_dt, payment_status, amount FROM insurance_event "
        "WHERE event_dt > %s AND event_dt <= %s ORDER BY event_dt ASC LIMIT %s",
        (since, until, limit),
    )
    return list(cur.fetchall())


def _ins_ops() -> tuple[InsertFn, FetchFn]:
    return _insurance_like_insert, _insurance_like_fetch


def _online_ins_ops() -> tuple[InsertFn, FetchFn]:
    return _insurance_like_insert, _insurance_like_fetch


def _hc_ops() -> tuple[InsertFn, FetchFn]:
    def insert_rows(cur: Any, n: int) -> None:
        for _ in range(n):
            hid = f"h{secrets.token_hex(6)}"
            uid = f"LS{random.randint(100000, 999999)}"
            cur.execute(
                "INSERT INTO healthcare_record (hc_id, ls_user_id, record_dt, bmi, weight_kg, exercise_type, "
                "exercise_duration_min, calories) VALUES (%s,%s,NOW(),%s,%s,%s,%s,%s)",
                (
                    hid,
                    uid,
                    Decimal("22.5"),
                    Decimal("70.0"),
                    random.choice(("RUNNING", "WALKING", "CYCLE")),
                    random.randint(10, 90),
                    random.randint(100, 800),
                ),
            )

    def fetch_rows(cur: Any, since: datetime, until: datetime, limit: int) -> list[dict[str, Any]]:
        cur.execute(
            "SELECT id, hc_id, ls_user_id, record_dt, bmi, weight_kg, exercise_type, exercise_duration_min, calories "
            "FROM healthcare_record WHERE record_dt > %s AND record_dt <= %s ORDER BY record_dt ASC LIMIT %s",
            (since, until, limit),
        )
        return list(cur.fetchall())

    return insert_rows, fetch_rows


def _hosp_ops() -> tuple[InsertFn, FetchFn]:
    def insert_rows(cur: Any, n: int) -> None:
        for _ in range(n):
            uid = f"LS{random.randint(100000, 999999)}"
            cur.execute(
                "INSERT INTO hospital_visit (hospital_id, ls_user_id, visit_dt, dept, diagnosis_code, treatment_cost) "
                "VALUES (%s,%s,NOW(),%s,%s,%s)",
                ("HOSP01", uid, "INTERNAL", "E11", random.randint(10000, 500000)),
            )

    def fetch_rows(cur: Any, since: datetime, until: datetime, limit: int) -> list[dict[str, Any]]:
        cur.execute(
            "SELECT visit_id, hospital_id, ls_user_id, visit_dt, dept, diagnosis_code, treatment_cost "
            "FROM hospital_visit WHERE visit_dt > %s AND visit_dt <= %s ORDER BY visit_dt ASC LIMIT %s",
            (since, until, limit),
        )
        return list(cur.fetchall())

    return insert_rows, fetch_rows


def _build_source_registry() -> None:
    bi, bf = _bank_ops()
    _SOURCE_REGISTRY["bank"] = (bi, bf, "transaction_dt")
    ci, cf = _card_ops()
    _SOURCE_REGISTRY["card"] = (ci, cf, "approval_dt")
    si, sf = _sec_ops()
    _SOURCE_REGISTRY["securities"] = (si, sf, "trade_dt")
    ii, inf = _ins_ops()
    _SOURCE_REGISTRY["insurance"] = (ii, inf, "event_dt")
    oi, ofn = _online_ins_ops()
    _SOURCE_REGISTRY["online_insurance"] = (oi, ofn, "event_dt")
    hi, hf = _hc_ops()
    _SOURCE_REGISTRY["healthcare"] = (hi, hf, "record_dt")
    hi2, hf2 = _hosp_ops()
    _SOURCE_REGISTRY["hospital"] = (hi2, hf2, "visit_dt")


_build_source_registry()


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


def _utc_naive_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def _source() -> str:
    v = os.environ.get("SOURCE_NAME", "group").strip()
    return v or "group"


def _normalize_source_name(name: str) -> str:
    s = (name or "").strip().lower().replace("-", "_")
    return s


def _record_count() -> int:
    try:
        n = int(os.environ.get("GROUP_BATCH_RECORDS", "100"))
    except ValueError:
        n = 100
    return max(1, min(n, 5000))


def _interval_sec() -> int:
    try:
        n = int(os.environ.get("GROUP_BATCH_INTERVAL_SEC", "600"))
    except ValueError:
        n = 600
    return max(10, n)


def _insert_per_tick() -> int:
    try:
        n = int(os.environ.get("GROUP_INSERT_PER_TICK", "20"))
    except ValueError:
        n = 20
    return max(0, min(n, 500))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def _mysql_configured() -> bool:
    return bool(
        os.environ.get("MYSQL_USER", "").strip()
        and os.environ.get("MYSQL_DATABASE", "").strip()
    )


def _use_mysql_export() -> bool:
    return _env_bool("GROUP_USE_MYSQL_EXPORT", True) and _mysql_configured() and pymysql is not None


def _mysql_connect() -> Any:
    if pymysql is None or DictCursor is None:
        raise RuntimeError("pymysql is not installed")
    host = os.environ.get("MYSQL_HOST", "127.0.0.1").strip()
    port = int(os.environ.get("MYSQL_PORT", "3306"))
    user = os.environ.get("MYSQL_USER", "").strip()
    password = os.environ.get("MYSQL_PASSWORD", "")
    database = os.environ.get("MYSQL_DATABASE", "").strip()
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=DictCursor,
    )


def _state_dir() -> Path:
    p = Path(os.environ.get("GROUP_STATE_DIR", "/opt/group-agent/state"))
    p.mkdir(parents=True, exist_ok=True)
    return p


def _watermark_path(source_name: str) -> Path:
    safe = _normalize_source_name(source_name).replace(os.sep, "_")
    return _state_dir() / f"watermark_{safe}.json"


def _load_watermark(source_name: str) -> datetime:
    path = _watermark_path(source_name)
    if not path.is_file():
        return datetime(2000, 1, 1)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        raw = data.get("last_event_time")
        if not raw:
            return datetime(2000, 1, 1)
        if isinstance(raw, str):
            if "T" in raw:
                return datetime.fromisoformat(raw.replace("Z", "+00:00").split("+")[0])
            return datetime.fromisoformat(raw)
    except (json.JSONDecodeError, OSError, ValueError) as e:
        LOG.warning("Watermark read failed (%s), using epoch: %s", path, e)
    return datetime(2000, 1, 1)


def _save_watermark(source_name: str, last_event: datetime) -> None:
    path = _watermark_path(source_name)
    body = {
        "source_name": source_name,
        "last_event_time": last_event.replace(microsecond=0).isoformat(sep=" ", timespec="seconds"),
    }
    path.write_text(json.dumps(body, indent=2), encoding="utf-8")


def _serialize_cell(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.replace(microsecond=0).isoformat(sep=" ", timespec="seconds")
    if isinstance(v, date) and not isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (bytes, memoryview)):
        return str(v)
    return v


def _serialize_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: _serialize_cell(v) for k, v in row.items()} for row in rows]


def _batch_payload_from_db(
    source_name: str,
    *,
    rows: list[dict[str, Any]],
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any]:
    now = _utc_now_iso()
    stamp = _today().replace("-", "")
    batch_id = hashlib.sha1(
        f"{source_name}:{now}:{len(rows)}:{window_end.isoformat()}".encode("utf-8")
    ).hexdigest()[:16]
    file_name = f"{source_name}_{stamp}_{batch_id}.json"
    return {
        "source_name": source_name,
        "timestamp": now,
        "file_name": file_name,
        "batch_id": batch_id,
        "record_count": len(rows),
        "records": rows,
        "export_window_start": window_start.isoformat(sep=" ", timespec="seconds"),
        "export_window_end": window_end.isoformat(sep=" ", timespec="seconds"),
    }


def _batch_payload_synthetic_legacy(source_name: str) -> dict[str, Any]:
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
    # 날짜만 쓰면 insurance / online_insurance 모두 하루 1키 → BatchLoader 가 duplicate skip.
    # DB 모드와 같이 배치마다 유니크한 객체 키를 쓴다.
    stamp = _today().replace("-", "")
    batch_id = hashlib.sha1(
        f"{source_name}:{now}:{len(rows)}:{secrets.token_hex(8)}".encode("utf-8")
    ).hexdigest()[:16]
    file_name = f"{source_name}_{stamp}_{batch_id}.json"
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
    # Lambda validates records only; strip export_window_* for smaller payload / strict consumers
    invoke_body = {
        k: v
        for k, v in payload.items()
        if k in ("source_name", "timestamp", "file_name", "batch_id", "record_count", "records")
    }
    event = {"Records": [{"body": json.dumps(invoke_body, ensure_ascii=False)}]}
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


def _max_time_from_rows(rows: list[dict[str, Any]], time_col: str) -> datetime | None:
    best: datetime | None = None
    for row in rows:
        v = row.get(time_col)
        if isinstance(v, datetime):
            if best is None or v > best:
                best = v
    return best


def send_once(source_name: str) -> int:
    raw_src = _normalize_source_name(source_name)
    if not _use_mysql_export():
        if _env_bool("GROUP_USE_MYSQL_EXPORT", True) and _mysql_configured() and pymysql is None:
            LOG.warning("GROUP_USE_MYSQL_EXPORT set but pymysql missing; install requirements.txt")
        payload = _batch_payload_synthetic_legacy(raw_src)
        _persist_local(payload)
        try:
            _invoke_lambda(payload)
            return 0
        except (BotoCoreError, ClientError, RuntimeError) as e:
            LOG.error("Batch send failed: %s", e)
            return 1

    if raw_src not in _SOURCE_REGISTRY:
        LOG.error("Unknown source_name=%r for MySQL export (expected one of %s)", raw_src, sorted(_SOURCE_REGISTRY))
        return 1

    insert_fn, fetch_fn, time_col = _SOURCE_REGISTRY[raw_src]
    until = _utc_naive_now()
    since = _load_watermark(raw_src)
    win_sec = max(0.0, (until - since).total_seconds())
    LOG.info(
        "DB export window (%s, %s] (~%.0fs since last successful batch; cap=%d rows)",
        since,
        until,
        win_sec,
        _record_count(),
    )
    n_ins = _insert_per_tick()

    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            if n_ins > 0:
                insert_fn(cur, n_ins)
            rows = fetch_fn(cur, since, until, _record_count())
    finally:
        conn.close()

    if not rows:
        LOG.info("DB export: 0 rows in (%s, %s]; advancing watermark", since, until)
        _save_watermark(raw_src, until)
        return 0

    serial = _serialize_records(rows)
    payload = _batch_payload_from_db(raw_src, rows=serial, window_start=since, window_end=until)
    _persist_local(payload)
    try:
        _invoke_lambda(payload)
    except (BotoCoreError, ClientError, RuntimeError) as e:
        LOG.error("Batch send failed: %s", e)
        return 1

    max_ts = _max_time_from_rows(rows, time_col)
    if max_ts is None:
        max_ts = until
    _save_watermark(raw_src, max_ts)
    LOG.info("DB export OK: %d rows, watermark -> %s", len(rows), max_ts)
    return 0


def run_sender(source_default: str | None = None) -> int:
    _setup_logging()
    raw = source_default or _source()
    source_name = _normalize_source_name(raw)
    interval = _interval_sec()
    LOG.info(
        "Starting group batch sender: source=%s interval=%ss mysql_export=%s run_once=%s",
        source_name,
        interval,
        _use_mysql_export(),
        _env_bool("GROUP_RUN_ONCE", False),
    )
    rc = send_once(source_name)
    if _env_bool("GROUP_RUN_ONCE", False):
        return rc
    while True:
        send_once(source_name)
        time.sleep(interval)


def main() -> int:
    return run_sender()


if __name__ == "__main__":
    sys.exit(main())
