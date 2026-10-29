# -*- coding: utf-8 -*-
"""
MQTT bridge (Jira CTIP-61).

ESP32 --MQTT--> broker --MQTT--> THIS SCRIPT --HTTP--> backend

Listens on nodes/+/telemetry and forwards each reading to the backend.
Two modes (set MODE below):
  "api"      -> POST {API_URL}/telemetry with X-Sensor-Key   (the contract way)
  "supabase" -> insert straight into the Supabase telemetry table
                and update sensor_nodes.last_seen            (temporary, works today)

Works on Python 2.7 and 3.x, and on paho-mqtt 1.x and 2.x. No extra packages.
Run with the VS Code play button, or:  python bridge.py

Put your keys in bridge_secrets.py (same folder, do NOT commit it):
    SUPABASE_SERVICE_KEY = "eyJ..."
    SENSOR_KEY = "..."
"""
from __future__ import print_function

import json
import time

import paho.mqtt.client as mqtt

try:  # Python 3
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError, URLError
except ImportError:  # Python 2.7
    from urllib2 import Request, urlopen, HTTPError, URLError

# ------------------------------------------------------------------ settings
MODE = "supabase"  # "supabase" for now; switch to "api" when POST /telemetry works

BROKER = "broker.hivemq.com"
PORT = 1883
TOPIC = "nodes/+/telemetry"

API_URL = "http://localhost:8000/v1"
SUPABASE_URL = "https://cgtvoirktzqjqaosslfh.supabase.co"

try:
    from bridge_secrets import SUPABASE_SERVICE_KEY
except ImportError:
    SUPABASE_SERVICE_KEY = ""
try:
    from bridge_secrets import SENSOR_KEY
except ImportError:
    SENSOR_KEY = ""

# Fields allowed by TelemetryReadingInput in the OpenAPI contract
ALLOWED = ("ts", "temperature", "humidity", "soil_moisture",
           "distance_cm", "motion_detected", "lat", "lng")


# ------------------------------------------------------------------ HTTP
def http(method, url, body, headers):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = Request(url, data=data, headers=headers)
    req.get_method = lambda: method
    try:
        resp = urlopen(req, timeout=10)
        return resp.getcode(), resp.read().decode("utf-8", "replace")
    except HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except URLError as e:
        return 0, str(e.reason)


def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": "Bearer " + SUPABASE_SERVICE_KEY,
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


# ------------------------------------------------------------------ forward
def send_to_api(node_id, reading):
    code, text = http("POST", API_URL + "/telemetry",
                      {"node_id": node_id, "readings": [reading]},
                      {"Content-Type": "application/json", "X-Sensor-Key": SENSOR_KEY})
    return code == 202, code, text


def send_to_supabase(node_id, reading):
    row = dict(reading)
    row["node_id"] = node_id
    code, text = http("POST", SUPABASE_URL + "/rest/v1/telemetry", row, supabase_headers())
    if code not in (200, 201, 204):
        return False, code, text
    # Keep last_seen fresh for the offline alert (CTIP-76)
    http("PATCH", SUPABASE_URL + "/rest/v1/sensor_nodes?id=eq." + node_id,
         {"last_seen": reading["ts"]}, supabase_headers())
    return True, code, text


def explain(code, text):
    if code == 0:
        return "cannot reach server (" + text + ")"
    if code == 401:
        return "bad key. Check bridge_secrets.py"
    if code == 501:
        return "POST /telemetry is not built yet. Use MODE = \"supabase\""
    if "23503" in text or "foreign key" in text:
        return "node_id is not registered in sensor_nodes. Put a real node UUID in secrets.h"
    if "22P02" in text:
        return "node_id is not a valid UUID"
    return "HTTP {}: {}".format(code, text[:200])


# ------------------------------------------------------------------ MQTT
def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print("MQTT connect failed, rc =", rc)
        return
    print("Connected to", BROKER, "- listening on", TOPIC, "- mode:", MODE)
    client.subscribe(TOPIC, qos=1)


def on_message(client, userdata, msg):
    node_id = msg.topic.split("/")[1]
    payload = msg.payload.decode("utf-8", "replace")
    try:
        data = json.loads(payload)
    except ValueError:
        print("[{}] skipped, bad JSON: {}".format(node_id, payload))
        return
    if "ts" not in data:
        print("[{}] skipped, no ts".format(node_id))
        return
    reading = dict((k, v) for k, v in data.items() if k in ALLOWED)

    if MODE == "api":
        ok, code, text = send_to_api(node_id, reading)
    else:
        ok, code, text = send_to_supabase(node_id, reading)

    if ok:
        print("[{}] saved {}".format(node_id, reading))
    else:
        print("[{}] NOT saved: {}".format(node_id, explain(code, text)))


def main():
    if MODE == "supabase" and not SUPABASE_SERVICE_KEY:
        print("Missing SUPABASE_SERVICE_KEY. Create bridge_secrets.py (see top of this file).")
        return
    if MODE == "api" and not SENSOR_KEY:
        print("Missing SENSOR_KEY. Create bridge_secrets.py (see top of this file).")
        return

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    except AttributeError:
        client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            client.connect(BROKER, PORT, 60)
            client.loop_forever()
        except KeyboardInterrupt:
            print("Stopped.")
            return
        except Exception as e:
            print("MQTT error:", e, "- retrying in 5 s")
            time.sleep(5)


if __name__ == "__main__":
    main()