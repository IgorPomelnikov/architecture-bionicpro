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

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
CDN_URL = os.getenv("CDN_URL", "http://localhost:8084/reports")


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
    query = f"""
        SELECT
            report_date,
            prosthetic_id,
            user_id,
            signal_count,
            signal_avg,
            response_time_avg,
            battery_avg,
            performance_score
        FROM prosthetic_reports_mv
        WHERE user_id = '{user_id}'
        ORDER BY report_date DESC
        LIMIT 100
        FORMAT JSONEachRow
    """

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(CLICKHOUSE_URL, data=query)
        response.raise_for_status()

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
