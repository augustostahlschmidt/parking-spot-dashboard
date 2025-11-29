# app.py
import os
import joblib
import json
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import StreamingResponse, FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, Dict, Any
from collections import deque, defaultdict
from statistics import median
from datetime import datetime, timedelta
import asyncio
import logging
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("parking_rf_model")

# ---------------------------
# Config
# ---------------------------
MODEL_PATH = os.environ.get("MODEL_PATH", "./parking_rf_model.pkl")
WINDOW = int(os.environ.get("WINDOW", 5))
DEBOUNCE_K = int(os.environ.get("DEBOUNCE_K", 1))
COOLDOWN_SECONDS = float(os.environ.get("COOLDOWN_SECONDS", 1.0))

# ---------------------------
# Payload model
# ---------------------------
class Reading(BaseModel):
    device_id: str
    timestamp: Optional[str] = None
    timestamp_ms: Optional[int] = None
    distance_cm: float

# ---------------------------
# Feature engine (igual ao seu)
# ---------------------------
class FeatureEngine:
    def __init__(self, window=5):
        self.window = window
        self.buffers = defaultdict(lambda: deque(maxlen=self.window))
        self.prev_distance = {}
        self.prev_dist_diff = {}

    def _to_dt(self, ts):
        if ts is None:
            return None
        if isinstance(ts, (int, float)):
            return datetime.utcfromtimestamp(ts / 1000.0)
        if isinstance(ts, str):
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                try:
                    return datetime.fromisoformat(ts)
                except Exception:
                    return None
        return None

    def add_reading(self, device_id: str, distance: float, timestamp=None):
        distance = float(distance)
        ts = self._to_dt(timestamp)

        buf = self.buffers[device_id]
        buf.append(distance)

        try:
            distance_filt = float(median(list(buf)))
        except Exception:
            distance_filt = distance

        prev_d = self.prev_distance.get(device_id)
        if prev_d is None:
            dist_diff = 0.0
        else:
            dist_diff = distance - prev_d

        prev_dd = self.prev_dist_diff.get(device_id, 0.0)
        dist_ddiff = dist_diff - prev_dd

        self.prev_distance[device_id] = distance
        self.prev_dist_diff[device_id] = dist_diff

        features = np.array([distance, distance_filt, dist_diff, dist_ddiff], dtype=float).reshape(1, -1)
        meta = {
            "device_id": device_id,
            "timestamp": ts,
            "raw_distance": distance,
            "distance_filt": distance_filt,
            "dist_diff": dist_diff,
            "dist_ddiff": dist_ddiff,
            "buffer_len": len(buf),
        }
        return features, meta

# ---------------------------
# Inference engine (igual ao seu)
# ---------------------------
class InferenceEngine:
    def __init__(self, model, feature_engine: FeatureEngine, debounce_k=1, cooldown_seconds=1.0):
        self.model = model
        self.fe = feature_engine
        self.debounce_k = max(1, int(debounce_k))
        self.cooldown = timedelta(seconds=float(cooldown_seconds))

        self._last_published = {}
        self._candidate_counts = defaultdict(int)
        self._last_publish_time = defaultdict(lambda: datetime.min)
        self._history = defaultdict(lambda: deque(maxlen=500))
        self._has_proba = hasattr(self.model, "predict_proba")

    def process(self, device_id: str, distance: float, timestamp=None):
        X, meta = self.fe.add_reading(device_id, distance, timestamp)
        pred = int(self.model.predict(X)[0])
        prob = None
        if self._has_proba:
            try:
                prob = float(self.model.predict_proba(X)[0][pred])
            except Exception:
                prob = None

        now = meta["timestamp"] or datetime.utcnow()

        last_pub = self._last_published.get(device_id)
        publish = False

        if last_pub is None:
            publish = True
            self._candidate_counts[device_id] = 1
        else:
            if pred == last_pub:
                self._candidate_counts[device_id] = 0
            else:
                self._candidate_counts[device_id] += 1
                if self._candidate_counts[device_id] >= self.debounce_k:
                    if datetime.utcnow() - self._last_publish_time[device_id] >= self.cooldown:
                        publish = True

        out = {
            "device_id": device_id,
            "timestamp": now.isoformat() if now else None,
            "distance_cm": float(distance),
            "distance_filt": float(meta["distance_filt"]),
            "dist_diff": float(meta["dist_diff"]),
            "dist_ddiff": float(meta["dist_ddiff"]),
            "predicted_occupied": int(pred),
            "prob": prob,
            "buffer_len": meta["buffer_len"]
        }

        self._history[device_id].append(out)

        if publish:
            self._last_publish_time[device_id] = datetime.utcnow()
            self._last_published[device_id] = pred
            self._candidate_counts[device_id] = 0
            return out, True
        else:
            return out, False

    def last_state(self, device_id: str):
        return {
            "last_pred": self._last_published.get(device_id),
            "last_publish_time": self._last_publish_time.get(device_id).isoformat() if self._last_publish_time.get(device_id) != datetime.min else None,
            "buffer": list(self.fe.buffers[device_id]),
            "recent": list(self._history[device_id])[-10:]
        }

# ---------------------------
# App and SSE broadcaster
# ---------------------------
app = FastAPI(title="Parking Inference API")

# CORS: em dev podemos usar "*" mas restrinja em produção
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Serve arquivos estáticos (index.html, styles.css, app.js, logs.html) a partir de ./static
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if not os.path.isdir(STATIC_DIR):
    logger.warning("Static directory %s not found. Crie a pasta 'static' e coloque index.html, app.js, styles.css, logs.html.", STATIC_DIR)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# load model at startup
MODEL = None
FE = FeatureEngine(window=WINDOW)
IE = None

# SSE queue
sse_subscribers = []

async def _notify_subscribers(payload: Dict[str, Any]):
    dead = []
    for q in list(sse_subscribers):
        try:
            await q.put(payload)
        except asyncio.QueueFull:
            logger.warning("SSE subscriber queue full, skipping")
        except Exception:
            dead.append(q)
    for d in dead:
        if d in sse_subscribers:
            sse_subscribers.remove(d)

@app.on_event("startup")
def startup_event():
    global MODEL, IE, FE
    if not os.path.exists(MODEL_PATH):
        logger.error("Model not found at %s. Put your .pkl at that path or set MODEL_PATH env var.", MODEL_PATH)
        raise RuntimeError("Model not found")
    logger.info("Loading model from %s", MODEL_PATH)
    MODEL = joblib.load(MODEL_PATH)
    IE = InferenceEngine(model=MODEL, feature_engine=FE, debounce_k=DEBOUNCE_K, cooldown_seconds=COOLDOWN_SECONDS)
    logger.info("Model loaded and InferenceEngine initialized")

# serve index.html on root
@app.get("/", response_class=HTMLResponse)
def root():
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path, media_type="text/html")
    return HTMLResponse("<h1>Index not found</h1>", status_code=404)

# POST /readings
@app.post("/readings")
async def readings(r: Reading):
    device = r.device_id
    ts = r.timestamp_ms if r.timestamp_ms is not None else r.timestamp
    dist = r.distance_cm
    try:
        out, published = IE.process(device, dist, ts)
    except Exception as e:
        logger.exception("processing error")
        raise HTTPException(status_code=500, detail=str(e))

    if published:
        # push to SSE subscribers (non-blocking)
        asyncio.create_task(_notify_subscribers(out))

    return {"published": bool(published), "result": out}

# GET /state/{device_id}
@app.get("/state/{device_id}")
def state(device_id: str):
    return IE.last_state(device_id)

# SSE endpoint
@app.get("/events")
async def events():
    q = asyncio.Queue(maxsize=100)
    sse_subscribers.append(q)

    async def event_generator(queue: asyncio.Queue):
        try:
            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=15.0)
                    data = json.dumps(payload)
                    yield f"data: {data}\n\n"
                except asyncio.TimeoutError:
                    # keep-alive comment to keep proxies from closing connection
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            logger.info("SSE client disconnected")
        finally:
            if q in sse_subscribers:
                sse_subscribers.remove(q)

    return StreamingResponse(event_generator(q), media_type="text/event-stream")

# simple healthcheck
@app.get("/health")
def health():
    return {"status": "ok"}