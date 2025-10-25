from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import json, asyncpg, asyncio

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from .config import API_KEY, DATABASE_URL, MAX_BODY_MB
from .tasks import enqueue_process_trip

# Rate limiter: e.g., 10 requests per minute per IP
limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter

pool: asyncpg.Pool | None = None

class Sample(BaseModel):
    timestamp: str
    uptime_ms: int
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    accuracy_m: Optional[float] = None
    speed_mps: Optional[float] = None
    accel: Optional[List[float]] = None
    gyro: List[float]

class Trip(BaseModel):
    user_id: str
    trip_id: str
    start_time: str
    samples: List[Sample]
    end_time: str
    sample_count: int

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(dsn=DATABASE_URL)

@app.exception_handler(RateLimitExceeded)
def ratelimit_handler(request, exc):
    return JSONResponse({"detail": "rate limit exceeded"}, status_code=429)

@app.get("/api/v1/health")
@limiter.limit("30/minute")
async def health(request: Request):  # <-- add request param
    return {"ok": True, "time": datetime.utcnow().isoformat() + "Z"}

@app.post("/api/v1/trips")
@limiter.limit("60/minute")
async def ingest_trip(request: Request, x_api_key: str = Header(None)):  # <-- rename req -> request
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")

    body = await request.body()  # <-- use request, not req
    if len(body) > MAX_BODY_MB * 1024 * 1024:
        raise HTTPException(status_code=413, detail="payload too large")

    try:
        trip = Trip.model_validate_json(body)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"invalid schema: {e}")
    
    # Insert idempotently + store raw JSON
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            "insert into users(user_id) values($1) on conflict do nothing", trip.user_id
        )
        await conn.execute(
            """insert into trips(trip_id, user_id, start_time, end_time, sample_count)
               values($1,$2,$3,$4,$5)
               on conflict (trip_id) do nothing""",
            trip.trip_id, trip.user_id, trip.start_time, trip.end_time, trip.sample_count
        )
        await conn.execute(
            """insert into trip_raw(trip_id, payload)
               values($1, $2::jsonb)
               on conflict (trip_id) do nothing""",
            trip.trip_id, json.dumps(json.loads(body))
        )

    # Kick off processing in the background worker
    enqueue_process_trip(trip.trip_id)

    return {"ok": True}

@app.get("/api/v1/clusters")
@limiter.limit("120/minute")
async def get_clusters(request: Request, min_conf: float = 0.4, since: Optional[str] = None):
    q = """select cluster_id, latitude, longitude, hits, users, last_ts,
                  avg_intensity, exposure, confidence, priority
           from pothole_clusters
           where confidence >= $1"""
    args = [min_conf]
    if since:
        q += " and last_ts >= $2"
        args.append(since)
    q += " order by priority desc limit 1000"
    async with pool.acquire() as conn:
        rows = await conn.fetch(q, *args)
    return [dict(r) for r in rows]

@app.get("/api/v1/leaderboard")
@limiter.limit("60/minute")
async def leaderboard(request: Request, limit: int = 50):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """select cluster_id, latitude, longitude, hits, users, last_ts,
                      avg_intensity, exposure, confidence, priority
               from pothole_clusters
               order by priority desc
               limit $1""", limit
        )
    return [dict(r) for r in rows]
