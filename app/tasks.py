from celery import Celery
import asyncio, asyncpg, json
from .config import BROKER_URL, RESULT_BACKEND, DATABASE_URL
from .processing import process_trip_payload

celery_app = Celery("ouaa", broker=BROKER_URL, backend=RESULT_BACKEND)

def enqueue_process_trip(trip_id: str):
    celery_app.send_task("ouaa.process_trip", args=[trip_id])

@celery_app.task(name="ouaa.process_trip")
def process_trip(trip_id: str):
    asyncio.run(_run(trip_id))

async def _run(trip_id: str):
    pool = await asyncpg.create_pool(dsn=DATABASE_URL)
    async with pool.acquire() as conn:
        raw = await conn.fetchrow("select payload from trip_raw where trip_id=$1", trip_id)
        if not raw:
            return
        payload = raw["payload"]
        dets, clusters = process_trip_payload(payload)  # returns (detections, clusters)
        # Store detections
        await conn.executemany(
            """insert into detections(trip_id, ts, latitude, longitude, intensity)
               values($1,$2,$3,$4,$5)""",
            [(trip_id, d["ts"], d["lat"], d["lon"], d["intensity"]) for d in dets]
        )
        # Upsert clusters
        for c in clusters:
            await conn.execute(
                """insert into pothole_clusters(cluster_id, latitude, longitude, hits, users,
                                                last_ts, avg_intensity, exposure, confidence, priority)
                   values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                   on conflict (cluster_id) do update set
                     latitude=excluded.latitude,
                     longitude=excluded.longitude,
                     hits=excluded.hits,
                     users=excluded.users,
                     last_ts=excluded.last_ts,
                     avg_intensity=excluded.avg_intensity,
                     exposure=excluded.exposure,
                     confidence=excluded.confidence,
                     priority=excluded.priority,
                     updated_at=now()""",
                c["cluster_id"], c["lat"], c["lon"], c["hits"], c["users"], c["last_ts"],
                c["avg_intensity"], c["exposure"], c["confidence"], c["priority"]
            )
    await pool.close()
