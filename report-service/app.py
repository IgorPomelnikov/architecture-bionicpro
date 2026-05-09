import asyncio
import base64
import json
import os
from datetime import datetime

import httpx
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.session import Session
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

app = FastAPI(title="BionicPRO Report Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123").rstrip("/")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://minio:9000").rstrip("/")
# URL, с которого браузер ходит в S3 (через nginx-CDN или напрямую на :9002).
# SigV4 привязан к Host и path — должен совпадать с тем, что видит MinIO (см. nginx Host).
S3_PRESIGN_ENDPOINT = os.getenv("S3_PRESIGN_ENDPOINT", "http://localhost:8084").rstrip("/")
S3_PRESIGN_EXPIRES_SECONDS = int(os.getenv("S3_PRESIGN_EXPIRES_SECONDS", "900"))
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "minioadmin")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "reports")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Структура ключей: {bucket}/user_{safe_id}/reports/{YYYY-MM-DD}_report.json —
# ключ строится только из JWT; чужой отчёт запросить нельзя. Доступ к файлу —
# через короткоживущий presigned URL (не анонимный бакет).

_bucket_initialized = False


def _s3_client():
    session = Session()
    return session.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def _s3_client_presign():
    session = Session()
    return session.client(
        "s3",
        endpoint_url=S3_PRESIGN_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


async def _ensure_bucket() -> None:
    global _bucket_initialized
    if _bucket_initialized:
        return

    def _sync() -> None:
        client = _s3_client()
        try:
            client.head_bucket(Bucket=S3_BUCKET)
            return
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in ("404", "NoSuchBucket"):
                raise
        try:
            client.create_bucket(Bucket=S3_BUCKET)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in (
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            ):
                raise

    await asyncio.to_thread(_sync)
    _bucket_initialized = True


def _ch_escape_string(value: str) -> str:
    """Экранирование литерала в одинарных кавычках для ClickHouse SQL."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _safe_key_segment(value: str) -> str:
    """Убираем символы, ломающие иерархию ключей в S3."""
    return value.replace("/", "_").replace("\\", "_")


def _extract_user_id_from_jwt(authorization: str | None) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")

    token = authorization.split(" ", 1)[1]
    parts = token.split(".")
    if len(parts) < 2:
        raise HTTPException(status_code=401, detail="Invalid token")

    payload_b64 = parts[1]
    payload_b64 += "=" * (-len(payload_b64) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(payload_b64).decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=401, detail=f"Invalid token payload: {exc}") from exc

    user_id = payload.get("preferred_username") or payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=403, detail="User id claim not found")
    return str(user_id)


def _report_object_key(user_id: str) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    safe_uid = _safe_key_segment(user_id)
    return f"user_{safe_uid}/reports/{today}_report.json"


async def _presigned_get_url(key: str) -> str:
    def _sync() -> str:
        return _s3_client_presign().generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": key},
            ExpiresIn=S3_PRESIGN_EXPIRES_SECONDS,
        )

    return await asyncio.to_thread(_sync)


async def _s3_object_exists(key: str) -> bool:
    def _sync() -> bool:
        try:
            _s3_client().head_object(Bucket=S3_BUCKET, Key=key)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    return await asyncio.to_thread(_sync)


async def _s3_put_report(key: str, body: bytes) -> None:
    def _sync() -> None:
        _s3_client().put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=body,
            ContentType="application/json; charset=utf-8",
            CacheControl="private, max-age=60",
        )

    await asyncio.to_thread(_sync)


async def _query_clickhouse(user_id: str) -> list[dict]:
    safe_uid = _ch_escape_string(user_id)
    query = f"""
        SELECT
            report_date,
            bionicpro_id,
            user_id,
            sum(signal_count) AS signal_count,
            if(sum(signal_count) = 0, 0, sum(signal_strength_sum) / sum(signal_count)) AS signal_avg,
            if(sum(signal_count) = 0, 0, sum(response_time_sum) / sum(signal_count)) AS response_time_avg,
            if(sum(signal_count) = 0, 0, sum(battery_sum) / sum(signal_count)) AS battery_avg,
            multiIf(
                sum(signal_count) = 0, 'needs_calibration',
                sum(response_time_sum) / sum(signal_count) < 100, 'excellent',
                sum(response_time_sum) / sum(signal_count) < 200, 'good',
                'needs_calibration'
            ) AS performance_score
        FROM bionicpro_reports_mart
        WHERE user_id = '{safe_uid}'
        GROUP BY user_id, bionicpro_id, report_date
        ORDER BY report_date DESC
        LIMIT 100
        FORMAT JSONEachRow
    """

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"{CLICKHOUSE_URL}/",
            params={"database": CLICKHOUSE_DATABASE},
            content=query.encode("utf-8"),
            headers={"Content-Type": "text/plain; charset=utf-8"},
        )
    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=(
                f"ClickHouse HTTP {response.status_code}: {response.text[:2000]}. "
                "Проверьте витрину bionicpro_reports_mart (CDC → Kafka → ClickHouse)."
            ),
        )

    rows = []
    for line in response.text.strip().splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


@app.get("/api/reports")
async def get_report(authorization: str | None = Header(default=None)):
    user_id = _extract_user_id_from_jwt(authorization)
    await _ensure_bucket()
    report_key = _report_object_key(user_id)

    if await _s3_object_exists(report_key):
        url = await _presigned_get_url(report_key)
        return JSONResponse(
            {
                "status": "cached",
                "url": url,
                "expires_in": S3_PRESIGN_EXPIRES_SECONDS,
                "key": report_key,
                "message": "Report in S3; temporary download URL (presigned)",
            }
        )

    rows = await _query_clickhouse(user_id)
    if not rows:
        return PlainTextResponse(content=f"Нет данных для пользователя {user_id}")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "user_id": user_id,
        "records": rows,
    }
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    await _s3_put_report(report_key, body)
    url = await _presigned_get_url(report_key)

    return JSONResponse(
        {
            "status": "generated",
            "url": url,
            "expires_in": S3_PRESIGN_EXPIRES_SECONDS,
            "key": report_key,
            "records": rows,
            "message": "Report stored in S3; temporary download URL (presigned)",
        }
    )


@app.get("/api/reports/health")
async def health():
    return {"status": "ok"}
