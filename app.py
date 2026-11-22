"""SmartAir AI backend: MQTT subscriber -> ML prediction -> alerts -> dashboard API.
Run: python app.py            (uses MQTT broker; start publisher.py separately)
     $env:SOURCE="local"      (PowerShell) -> skip MQTT, backend runs its own sensor node
     $env:MODE="replay"       -> use CPCB dataset instead of live data"""
import json, os, threading, time
from collections import deque
from datetime import datetime
import joblib, pandas as pd
from flask import Flask, jsonify, render_template, request
import paho.mqtt.client as mqtt
from common import (BROKER, PORT, TOPIC, DEFAULT_CITY, CITY_COORDS, FEATURES, CORE,
                    MODEL_DIR, category, DeltaModel)
from live_feed import run_cities

app = Flask(__name__)
if not (MODEL_DIR / "model.joblib").exists():
    raise SystemExit("Model not found. Run  python train.py  once, then start app.py again.")
model = joblib.load(MODEL_DIR / "model.joblib")
metrics = json.load(open(MODEL_DIR / "metrics.json"))

lock = threading.Lock()
readings = {}          # city -> deque of readings
last_level = {}        # city -> last alert level
alerts = deque(maxlen=15)
state = {"source": "starting", "data_mode": ""}
WARN, CRIT = 200, 300   # alert thresholds on PREDICTED next-day AQI

def ingest(p):
    """Turn one raw sensor reading into features, predict tomorrow's AQI, raise alerts."""
    city = p.get("city", "?")
    with lock:
        dq = readings.setdefault(city, deque(maxlen=120))
        # keep readings sorted by date, one per day (a re-sent day replaces the old one)
        items = sorted((r for r in dq if r["date"] != p["date"]), key=lambda r: r["date"])
        prev = [r for r in items if r["date"] < p["date"]]      # only days BEFORE this reading
        cur = float(p["AQI"])
        hist = [r["AQI"] for r in prev]
        lag = lambda k: hist[-k] if len(hist) >= k else (hist[0] if hist else cur)
        last = prev[-1] if prev else None
        d = pd.Timestamp(p["date"])
        row = {k: float(p[k]) for k in CORE}
        row.update({"Month": d.month, "DayOfWeek": d.day_of_week, "AQI": cur,
                    "AQI_lag1": lag(1), "AQI_lag2": lag(2), "AQI_lag3": lag(3),
                    "PM25_lag1": last["PM2.5"] if last else float(p["PM2.5"]),
                    "PM10_lag1": last["PM10"] if last else float(p["PM10"]),
                    "AQI_roll3": sum(([cur] + hist[-2:])) / len([cur] + hist[-2:]),
                    "AQI_roll7": sum(([cur] + hist[-6:])) / len([cur] + hist[-6:])})
        pred = min(500.0, max(0.0, float(model.predict(pd.DataFrame([row])[FEATURES])[0])))
        rec = {**{k: float(p[k]) for k in CORE}, "AQI": cur, "date": p["date"],
               "ts": datetime.now().strftime("%H:%M:%S"), "city": city,
               "predicted": round(pred, 1)}
        state["data_mode"] = p.get("data_mode", "")
        items.append(rec)
        items.sort(key=lambda r: r["date"])
        dq.clear(); dq.extend(items[-120:])
        level = "critical" if pred > CRIT else "warning" if pred > WARN else "ok"
        if level != "ok" and level != last_level.get(city, "ok"):
            alerts.appendleft({"time": rec["ts"], "date": p["date"], "city": city,
                               "level": level, "message":
                               f"{city}: predicted AQI {pred:.0f} ({category(pred)['name']}) for tomorrow - {category(pred)['advice']}"})
        last_level[city] = level

def run_local_simulator():
    """No broker: backend runs its own sensor nodes. CITIES=all (default) or e.g. CITIES=Bhopal"""
    state["source"] = "Local simulator (no broker)"
    sel = os.getenv("CITIES", "all")
    cities = list(CITY_COORDS) if sel.lower() == "all" else [c.strip() for c in sel.split(",")]
    run_cities(cities, ingest, 2.0, fallback_replay=len(cities) == 1)

def start_mqtt():
    def on_message(c, u, msg):
        try: ingest(json.loads(msg.payload))
        except Exception as e: print("bad payload:", e)
    def on_connect(c, u, flags, rc, props=None):
        c.subscribe(TOPIC); state["source"] = f"MQTT {BROKER} | {TOPIC}"
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    c.on_connect, c.on_message = on_connect, on_message
    try:
        c.connect(BROKER, PORT, 60); c.loop_start()
    except Exception as e:
        print("MQTT connect failed -> using local simulator:", e)
        run_local_simulator()

@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/live")
def api_live():
    want = request.args.get("city")
    with lock:
        cities = sorted(c for c in readings if readings[c])
        city = want if want in cities else (DEFAULT_CITY if DEFAULT_CITY in cities else (cities[0] if cities else None))
        hist = list(readings[city]) if city else []
        latest = hist[-1] if hist else None
        summary = []
        for c in cities:
            r = readings[c][-1]
            summary.append({"city": c, "AQI": r["AQI"], "predicted": r["predicted"], "date": r["date"],
                            "cat_now": category(r["AQI"]), "cat_pred": category(r["predicted"])})
        out = {"source": state["source"], "data_mode": state["data_mode"], "city": city,
               "history": hist[-60:], "alerts": list(alerts), "latest": latest, "cities": summary}
        if latest:
            out["cat_now"] = category(latest["AQI"]); out["cat_pred"] = category(latest["predicted"])
    return jsonify(out)

@app.route("/api/metrics")
def api_metrics(): return jsonify(metrics)

if __name__ == "__main__":
    if os.getenv("SOURCE", "mqtt").lower() == "local":
        run_local_simulator()
    else:
        start_mqtt()
    port = int(os.getenv("PORT", "5001"))
    print(f"Open http://127.0.0.1:{port}")
    app.run(port=port, debug=False)
