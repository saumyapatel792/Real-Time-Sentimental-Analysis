"""
Real-Time Sentiment Analysis Dashboard Server
FastAPI + WebSockets + Kafka Stream Consumer & Live Generator Hub
"""

import os
import sys
import json
import time
import asyncio
import threading
from datetime import datetime
from collections import deque
from typing import List, Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

# Add spark path to import sentiment engine
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "spark"))
from sentiment_engine import analyze_sentiment

app = FastAPI(title="Real-Time Sentiment Analysis Stream")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
os.makedirs(STATIC_DIR, exist_ok=True)

# In-memory streaming state
MAX_HISTORY = 100
stream_history = deque(maxlen=MAX_HISTORY)
connected_clients: List[WebSocket] = []

metrics = {
    "total": 0,
    "positive": 0,
    "negative": 0,
    "neutral": 0,
    "polarity_sum": 0.0,
    "keyword_counts": {},
    "events_last_minute": deque(maxlen=60),
    "started_at": datetime.utcnow().isoformat()
}

# Auto-stream generator state
is_streaming = True
stream_speed = 2.0  # seconds per event
stream_thread = None

PRESET_STREAM_DATA = [
    # User test sentences
    {"text": "This movie was fantastic!", "source": "MovieReviews", "user": "alice_w"},
    {"text": "I really enjoyed this product.", "source": "ProductReviews", "user": "mark_dev"},
    {"text": "The movie was boring.", "source": "MovieReviews", "user": "cinemafan99"},
    {"text": "This was a terrible experience.", "source": "CustomerCare", "user": "sarah_k"},
    # Extended continuous data
    {"text": "Apache Spark MLlib and Kafka streaming make big data so easy!", "source": "TechTwitter", "user": "spark_hero"},
    {"text": "I hate waiting in line for 2 hours, worst customer service ever.", "source": "SupportFeed", "user": "angry_traveler"},
    {"text": "The pizza was absolutely delicious and freshly baked!", "source": "FoodBlog", "user": "chef_mario"},
    {"text": "My phone is lagging and battery drains way too fast.", "source": "GadgetReviews", "user": "tech_user88"},
    {"text": "Super fast delivery and flawless packaging. 5 stars!", "source": "AmazonStore", "user": "happy_shopper"},
    {"text": "The application crashed again after the latest update.", "source": "AppStoreReviews", "user": "bug_reporter"},
    {"text": "The weather forecast predicts light rain in the afternoon.", "source": "NewsWire", "user": "meteo_bot"},
    {"text": "Outstanding presentation by the team today, great job!", "source": "SlackWorkspace", "user": "lead_engineer"},
    {"text": "Terrible battery life and poor screen quality, completely regret buying.", "source": "HardwareReviews", "user": "buyer_301"},
    {"text": "I love the new dark mode interface, it looks stunning!", "source": "DesignCommunity", "user": "ui_designer"}
]


class TextRequest(BaseModel):
    text: str
    source: Optional[str] = "ManualInput"
    user: Optional[str] = "LiveTester"


class StreamConfig(BaseModel):
    enabled: bool
    speed: Optional[float] = 2.0


def process_and_record_message(text: str, source="LiveFeed", user="user_live", msg_id=None):
    """Run sentiment analysis and update in-memory metrics"""
    global metrics
    if not msg_id:
        msg_id = f"evt_{int(time.time() * 1000) % 1000000:06d}"

    analysis = analyze_sentiment(text)
    
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    full_event = {
        "id": msg_id,
        "text": text,
        "source": source,
        "user": user,
        "timestamp": timestamp,
        "sentiment": analysis["sentiment"],
        "emoji": analysis["emoji"],
        "label": analysis["label"],
        "polarity": analysis["polarity"],
        "confidence": analysis["confidence"],
        "positive_score": analysis["positive_score"],
        "negative_score": analysis["negative_score"],
        "keywords": analysis["keywords"]
    }

    # Update metrics
    metrics["total"] += 1
    if analysis["sentiment"] == "Positive":
        metrics["positive"] += 1
    elif analysis["sentiment"] == "Negative":
        metrics["negative"] += 1
    else:
        metrics["neutral"] += 1

    metrics["polarity_sum"] += analysis["polarity"]
    metrics["events_last_minute"].append(time.time())

    for kw in analysis["keywords"]:
        metrics["keyword_counts"][kw] = metrics["keyword_counts"].get(kw, 0) + 1

    stream_history.appendleft(full_event)
    return full_event


async def broadcast_event(event: dict):
    """Broadcast new event to all active WebSocket connections"""
    if not connected_clients:
        return
    msg_json = json.dumps({"type": "NEW_EVENT", "data": event, "metrics": get_metrics_payload()})
    
    to_remove = []
    for ws in connected_clients:
        try:
            await ws.send_text(msg_json)
        except Exception:
            to_remove.append(ws)
            
    for ws in to_remove:
        if ws in connected_clients:
            connected_clients.remove(ws)


def get_metrics_payload():
    """Format aggregated metrics"""
    now = time.time()
    # Filter events in last 60 seconds
    recent_events = [t for t in metrics["events_last_minute"] if now - t <= 60]
    throughput_per_sec = round(len(recent_events) / 60.0, 2)
    
    total = metrics["total"]
    avg_polarity = round(metrics["polarity_sum"] / total, 3) if total > 0 else 0.0
    pos_pct = round((metrics["positive"] / total) * 100, 1) if total > 0 else 0.0
    neg_pct = round((metrics["negative"] / total) * 100, 1) if total > 0 else 0.0
    neu_pct = round((metrics["neutral"] / total) * 100, 1) if total > 0 else 0.0

    # Top keywords
    sorted_keywords = sorted(metrics["keyword_counts"].items(), key=lambda x: x[1], reverse=True)[:10]

    return {
        "total": total,
        "positive": metrics["positive"],
        "negative": metrics["negative"],
        "neutral": metrics["neutral"],
        "positive_pct": pos_pct,
        "negative_pct": neg_pct,
        "neutral_pct": neu_pct,
        "avg_polarity": avg_polarity,
        "throughput_eps": throughput_per_sec,
        "is_streaming": is_streaming,
        "stream_speed": stream_speed,
        "top_keywords": sorted_keywords
    }


# Background loop for continuous streaming
loop_ref = None

def background_stream_worker():
    """Continuous generator simulating real-time Kafka/Spark pipeline flow"""
    global is_streaming, stream_speed
    idx = 0
    while True:
        if is_streaming and loop_ref:
            item = PRESET_STREAM_DATA[idx % len(PRESET_STREAM_DATA)]
            event = process_and_record_message(item["text"], item["source"], item["user"])
            
            # Schedule broadcast on asyncio loop
            asyncio.run_coroutine_threadsafe(broadcast_event(event), loop_ref)
            idx += 1
        time.sleep(max(0.2, stream_speed))


try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

@app.on_event("startup")
async def startup_event():
    global loop_ref, stream_thread
    loop_ref = asyncio.get_event_loop()
    # Initialize with the user's test sentences
    for item in PRESET_STREAM_DATA[:4]:
        process_and_record_message(item["text"], item["source"], item["user"])
    
    # Start stream thread
    stream_thread = threading.Thread(target=background_stream_worker, daemon=True)
    stream_thread.start()
    print("[*] Real-Time Sentiment Streaming Hub Started on http://localhost:8050")


@app.get("/")
async def get_dashboard():
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return HTMLResponse("<h1>Dashboard index.html loading...</h1>")


@app.post("/api/predict")
async def predict_sentiment(req: TextRequest):
    """Immediate classification and streaming injection"""
    event = process_and_record_message(req.text, req.source, req.user)
    await broadcast_event(event)
    return {
        "status": "success",
        "result": event,
        "metrics": get_metrics_payload()
    }


@app.get("/api/history")
async def get_history():
    return {
        "history": list(stream_history),
        "metrics": get_metrics_payload()
    }


@app.get("/api/metrics")
async def get_metrics():
    return get_metrics_payload()


@app.post("/api/stream/control")
async def control_stream(cfg: StreamConfig):
    global is_streaming, stream_speed
    is_streaming = cfg.enabled
    if cfg.speed is not None and cfg.speed > 0:
        stream_speed = cfg.speed
    return {"status": "success", "is_streaming": is_streaming, "speed": stream_speed}


@app.post("/api/clear")
async def clear_history():
    global metrics, stream_history
    stream_history.clear()
    metrics["total"] = 0
    metrics["positive"] = 0
    metrics["negative"] = 0
    metrics["neutral"] = 0
    metrics["polarity_sum"] = 0.0
    metrics["keyword_counts"].clear()
    metrics["events_last_minute"].clear()
    return {"status": "cleared", "metrics": get_metrics_payload()}


@app.websocket("/ws/stream")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    # Send initial state
    init_payload = {
        "type": "INITIAL_STATE",
        "history": list(stream_history),
        "metrics": get_metrics_payload()
    }
    await websocket.send_text(json.dumps(init_payload))
    try:
        while True:
            data = await websocket.receive_text()
            # Parse client ping or command if any
            try:
                cmd = json.loads(data)
                if cmd.get("action") == "SEND_TEXT":
                    evt = process_and_record_message(cmd.get("text", ""), cmd.get("source", "WebClient"), "dashboard_user")
                    await broadcast_event(evt)
            except Exception:
                pass
    except WebSocketDisconnect:
        if websocket in connected_clients:
            connected_clients.remove(websocket)


# Mount static assets directory
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def run_server(host="0.0.0.0", port=8050):
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    run_server()
