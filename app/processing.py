import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from datetime import datetime
import hashlib

def _to_dt(s):  # ISO8601 → pandas datetime
    return pd.to_datetime(s, utc=True, errors="coerce")

def process_trip_payload(payload: dict):
    # Flatten samples
    samples = payload.get("samples", [])
    if not samples:
        return [], []

    df = pd.DataFrame(samples)
    # Keep only rows with gyro present; attach lat/lon if any
    # Build magnitudes
    def mag(arr):
        if arr is None or len(arr) != 3: return np.nan
        return float(np.sqrt(arr[0]**2 + arr[1]**2 + arr[2]**2))
    df["ts"] = df["timestamp"].apply(_to_dt)
    df["accel_mag"] = df["accel"].apply(mag)
    df["gyro_mag"]  = df["gyro"].apply(mag)
    df["lat"] = df.get("latitude")
    df["lon"] = df.get("longitude")

    # Rolling z-score on accel_mag (window ~10 samples) gated by gyro activity
    df = df.sort_values("ts").reset_index(drop=True)
    window = 10
    roll_mean = df["accel_mag"].rolling(window, min_periods=5).mean()
    roll_std  = df["accel_mag"].rolling(window, min_periods=5).std()
    z = (df["accel_mag"] - roll_mean) / (roll_std.replace(0, np.nan))
    df["z"] = z
    gyro_thresh = np.nanpercentile(df["gyro_mag"], 60)
    candidates = df[(df["z"] > 2.5) & (df["gyro_mag"] >= gyro_thresh)]

    # Debounce: keep events >= 0.5s apart
    detections = []
    last_ts = None
    for _, r in candidates.iterrows():
        if pd.isna(r["ts"]): continue
        if last_ts is not None and (r["ts"] - last_ts).total_seconds() < 0.5: 
            continue
        detections.append({
            "ts": r["ts"].to_pydatetime(),
            "lat": None if pd.isna(r["lat"]) else float(r["lat"]),
            "lon": None if pd.isna(r["lon"]) else float(r["lon"]),
            "intensity": float(r["z"]) if not np.isnan(r["z"]) else 0.0
        })
        last_ts = r["ts"]

    # Only keep detections with coordinates
    det_geo = [d for d in detections if d["lat"] is not None and d["lon"] is not None]
    if not det_geo:
        return detections, []

    # DBSCAN on lat/lon with ~12m eps (approx deg per meter ~ 1/111111)
    eps_deg = 12.0 / 111111.0
    X = np.array([[d["lat"], d["lon"]] for d in det_geo])
    clustering = DBSCAN(eps=eps_deg, min_samples=3, metric="euclidean").fit(X)
    labels = clustering.labels_
    clusters = []
    for lbl in set(labels):
        if lbl == -1: 
            continue
        pts = [det_geo[i] for i in range(len(det_geo)) if labels[i] == lbl]
        if not pts: 
            continue
        lat = float(np.mean([p["lat"] for p in pts]))
        lon = float(np.mean([p["lon"] for p in pts]))
        hits = len(pts)
        users = None  # need cross-trip aggregation; placeholder 1 for per-trip
        last_ts = max(p["ts"] for p in pts)
        avg_int = float(np.mean([p["intensity"] for p in pts if p["intensity"] is not None]))
        freshness = max(0.0, 1.0 - (datetime.utcnow() - last_ts).days / 60.0)
        confidence = 0.3*min(hits,10)/10 + 0.6*freshness + 0.1*avg_int
        exposure = 0.0  # fill later with route exposure analytics
        priority = 0.6*confidence + 0.4*exposure

        # Stable cluster_id hash
        cid_src = f"{round(lat,6)}:{round(lon,6)}"
        cluster_id = "pc_" + hashlib.sha1(cid_src.encode()).hexdigest()[0:10]

        clusters.append({
            "cluster_id": cluster_id,
            "lat": lat, "lon": lon,
            "hits": hits, "users": 1,
            "last_ts": last_ts,
            "avg_intensity": avg_int,
            "exposure": exposure,
            "confidence": confidence,
            "priority": priority
        })

    return detections, clusters
