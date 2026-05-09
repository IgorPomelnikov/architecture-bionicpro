import base64
import json
import os
from datetime import datetime

import httpx
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
CDN_URL = os.getenv("CDN_URL", "http://localhost:8084/reports")


def _ch_escape_string(value: str) -> str:
    """Экранирование литерала в одинарных кавычках для ClickHouse SQL."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


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


async def _query_clickhouse(user_id: str) -> list[dict]:
    safe_uid = _ch_escape_string(user_id)
    query = f"""
        SELECT
            report_date,
            bionicpro_id,
            user_id,
            signal_count,
            signal_avg,
            response_time_avg,
            battery_avg,
            performance_score
        FROM bionicpro_reports_mart
        WHERE user_id = '{safe_uid}'
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
                "Проверьте, что таблица bionicpro_reports_mart есть (DAG crm_to_clickhouse)."
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
    rows = await _query_clickhouse(user_id)

    if not rows:
        return PlainTextResponse(content=f"Нет данных для пользователя {user_id}")

    today = datetime.now().strftime("%Y-%m-%d")
    report_key = f"user_{user_id}/reports/{today}_report.json"
    return JSONResponse(
        {
            "status": "generated",
            "url": f"{CDN_URL}/{report_key}",
            "records": rows,
            "message": "Report generated from OLAP by authorized user",
        }
    )


@app.get("/api/reports/health")
async def health():
    return {"status": "ok"}
