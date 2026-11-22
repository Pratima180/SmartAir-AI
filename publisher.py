"""IoT sensor node(s) (publisher). Sends air-quality readings to the MQTT broker.
MODE=live   -> real recent data (Open-Meteo/CAMS), today's value refreshed every 5 min
MODE=replay -> CPCB dataset replay (offline demo)
Usage:  python publisher.py --city Bhopal          (one sensor node)
        python publisher.py --all                  (12 sensor nodes = 12 cities)"""
import argparse, json, time
import paho.mqtt.client as mqtt
from common import BROKER, PORT, TOPIC, DEFAULT_CITY, CITY_COORDS
from live_feed import run_cities

ap = argparse.ArgumentParser()
ap.add_argument("--city", default=DEFAULT_CITY)
ap.add_argument("--all", action="store_true", help="publish all cities in CITY_COORDS")
ap.add_argument("--interval", type=float, default=2.0, help="seconds between readings per city")
a = ap.parse_args()

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(BROKER, PORT, 60)
client.loop_start()
cities = list(CITY_COORDS) if a.all else [a.city]
print(f"Publishing {len(cities)} sensor node(s) to {BROKER}:{PORT}  topic={TOPIC}")

def emit(p):
    client.publish(TOPIC, json.dumps(p), qos=0)
    print("sent", p["city"], p["date"], "AQI", p["AQI"], "PM2.5", p["PM2.5"], "|", p["data_mode"])

run_cities(cities, emit, a.interval, fallback_replay=not a.all)
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
