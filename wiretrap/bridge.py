"""
node_gateway.py - MQTT bridge + laptop webcam in ONE script
Jira: CTIP-61 (MQTT bridge) and CTIP-62 (laptop webcam capture)

ESP32 --MQTT--> broker --MQTT--> THIS SCRIPT
                                   |-- nodes/+/telemetry -> backend (Supabase or API)
                                   |-- nodes/+/trigger   -> webcam photo -> POST image

Telemetry (same as bridge.py), set MODE below:
  "supabase" -> insert into the Supabase telemetry table and update
                sensor_nodes.last_seen                      (temporary, works today)
  "api"      -> POST {API_URL}/telemetry with X-Sensor-Key  (the contract way)

Camera (same as capture.py):
  One photo per trigger, at most one per node every COOLDOWN_S seconds.
  Raw JPEG (2 MB max) -> POST {API_URL}/sensor-nodes/{node_id}/images
  Until that endpoint works, photos wait in captures/pending/ and are retried.

The parts run separately: if the camera fails, telemetry keeps flowing.

Run (PowerShell, Python 3):
    .venv\\Scripts\\Activate.ps1
    pip install opencv-python paho-mqtt requests
    python node_gateway.py               # bridge + camera
    python node_gateway.py --no-camera   # bridge only
    python node_gateway.py --test        # take one test photo and exit

Keys in bridge_secrets.py (same folder, do NOT commit it):
    SUPABASE_SERVICE_KEY = "eyJ..."
    SENSOR_KEY = "..."
"""
import json
import os
import queue
import re
import shutil
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import requests

# ================================================================ Settings
MODE = "supabase"   # "supabase" for now; switch to "api" when POST /telemetry works

BROKER = "broker.hivemq.com"
PORT = 1883
TELEMETRY_TOPIC = "nodes/+/telemetry"
TRIGGER_TOPIC = "nodes/+/trigger"

API_URL = "http://localhost:8000/v1"   # the team's API, used for telemetry (api mode) and images
SUPABASE_URL = "https://cgtvoirktzqjqaosslfh.supabase.co"
HTTP_TIMEOUT_S = 10
UPLOAD_TIMEOUT_S = 15
RETRY_EVERY_S = 30          # retry pending photos this often

CAMERA_INDEX = 0            # 0 = built-in laptop webcam, try 1 if it fails
FRAME_WIDTH = 1280
FRAME_HEIGHT = 720
JPEG_QUALITY = 85
MAX_IMAGE_BYTES = 2 * 1024 * 1024   # contract limit: 2 MB
COOLDOWN_S = 5              # at most one photo per node every 5 s

# Fields allowed by TelemetryReadingInput in the OpenAPI contract
ALLOWED = ("ts", "temperature", "humidity", "soil_moisture",
           "distance_cm", "motion_detected", "lat", "lng")

HERE = os.path.dirname(os.path.abspath(__file__))
PENDING_DIR = os.path.join(HERE, "captures", "pending")
UPLOADED_DIR = os.path.join(HERE, "captures", "uploaded")
FAILED_DIR = os.path.join(HERE, "captures", "failed")
TEST_DIR = os.path.join(HERE, "captures", "test")

try:
    from bridge_secrets import SUPABASE_SERVICE_KEY
except ImportError:
    SUPABASE_SERVICE_KEY = ""
try:
    from bridge_secrets import SENSOR_KEY
except ImportError:
    SENSOR_KEY = ""

UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def log(part, text):
    """One log style for both parts: 12:34:56 [TELEMETRY] ..."""
    print("%s [%s] %s" % (time.strftime("%H:%M:%S"), part, text), flush=True)


def utc_stamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ================================================================ Telemetry (CTIP-61)
def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": "Bearer " + SUPABASE_SERVICE_KEY,
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def http_json(method, url, body, headers):
    """Returns (status_code, text). status 0 = server not reachable."""
    try:
        r = requests.request(method, url, json=body, headers=headers, timeout=HTTP_TIMEOUT_S)
        return r.status_code, r.text
    except requests.RequestException as e:
        return 0, e.__class__.__name__


def send_to_api(node_id, reading):
    code, text = http_json("POST", API_URL + "/telemetry",
                           {"node_id": node_id, "readings": [reading]},
                           {"Content-Type": "application/json", "X-Sensor-Key": SENSOR_KEY})
    return code == 202, code, text


def send_to_supabase(node_id, reading):
    row = dict(reading)
    row["node_id"] = node_id
    code, text = http_json("POST", SUPABASE_URL + "/rest/v1/telemetry", row, supabase_headers())
    if code not in (200, 201, 204):
        return False, code, text
    # Keep last_seen fresh for the offline alert (CTIP-76)
    http_json("PATCH", SUPABASE_URL + "/rest/v1/sensor_nodes?id=eq." + node_id,
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


def handle_telemetry(node_id, payload):
    try:
        data = json.loads(payload)
    except ValueError:
        log("TELEMETRY", "%s skipped, bad JSON: %s" % (node_id, payload))
        return
    if not isinstance(data, dict) or "ts" not in data:
        log("TELEMETRY", "%s skipped, no ts" % node_id)
        return
    reading = dict((k, v) for k, v in data.items() if k in ALLOWED)

    if MODE == "api":
        ok, code, text = send_to_api(node_id, reading)
    else:
        ok, code, text = send_to_supabase(node_id, reading)

    if ok:
        log("TELEMETRY", "%s saved %s" % (node_id, reading))
    else:
        log("TELEMETRY", "%s NOT saved: %s" % (node_id, explain(code, text)))


def telemetry_worker(q):
    """Runs in its own thread so slow HTTP never blocks MQTT or the camera."""
    while True:
        node_id, payload = q.get()
        try:
            handle_telemetry(node_id, payload)
        except Exception as e:   # never let one bad reading stop the bridge
            log("TELEMETRY", "%s ERROR: %s" % (node_id, e))


# ================================================================ Camera (CTIP-62)
class Camera:
    """Keeps the webcam open so a photo is taken fast after a trigger."""

    def __init__(self, index):
        import cv2   # imported here so --no-camera works without OpenCV
        self.cv2 = cv2
        backend = cv2.CAP_DSHOW if sys.platform.startswith("win") else cv2.CAP_ANY
        self.cap = cv2.VideoCapture(index, backend)
        if not self.cap.isOpened():
            raise RuntimeError(
                "Cannot open webcam %d. Close other apps using it (Zoom, Teams, "
                "Camera app) or try CAMERA_INDEX = 1." % index)
        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, FRAME_WIDTH)
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, FRAME_HEIGHT)
        for _ in range(10):         # let auto exposure settle
            self.cap.read()

    def take_jpeg(self):
        cv2 = self.cv2
        for _ in range(4):          # drop old buffered frames, get a fresh one
            self.cap.grab()
        ok, frame = self.cap.read()
        if not ok or frame is None:
            raise RuntimeError("Webcam returned no frame")

        quality = JPEG_QUALITY
        while True:
            ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
            if not ok:
                raise RuntimeError("JPEG encoding failed")
            data = buf.tobytes()
            if len(data) <= MAX_IMAGE_BYTES or quality <= 30:
                return data
            quality -= 10

    def close(self):
        self.cap.release()


def save_photo(folder, node_id, captured_at, source, data):
    """Save the photo first, so it is never lost if the upload fails."""
    safe_ts = captured_at.replace(":", "")  # Windows file names cannot contain ':'
    name = "%s__%s__%s__%s.jpg" % (node_id, safe_ts, source, uuid.uuid4().hex[:6])
    path = os.path.join(folder, name)
    with open(path, "wb") as f:
        f.write(data)
    return path


def parse_pending_name(path):
    """Get node_id and captured_at back from a pending file name."""
    parts = os.path.basename(path)[:-4].split("__")
    node_id, safe_ts = parts[0], parts[1]
    # 2026-10-30T031900Z -> 2026-10-30T03:19:00Z
    date, clock = safe_ts.split("T")
    captured_at = "%sT%s:%s:%s" % (date, clock[0:2], clock[2:4], clock[4:])
    return node_id, captured_at


RETRYABLE = {404, 408, 429, 501, 502, 503, 504}


def upload(path):
    """Returns True if done (uploaded or given up), False to retry later."""
    node_id, captured_at = parse_pending_name(path)
    name = os.path.basename(path)
    url = "%s/sensor-nodes/%s/images" % (API_URL.rstrip("/"), node_id)
    with open(path, "rb") as f:
        data = f.read()
    headers = {
        "Content-Type": "image/jpeg",
        "X-Sensor-Key": SENSOR_KEY,
        "X-Captured-At": captured_at,
    }
    try:
        r = requests.post(url, data=data, headers=headers, timeout=UPLOAD_TIMEOUT_S)
    except requests.RequestException as e:
        log("CAMERA", "  NOT uploaded (API unreachable: %s). Kept in pending/" % e.__class__.__name__)
        return False

    if 200 <= r.status_code < 300:
        shutil.move(path, os.path.join(UPLOADED_DIR, name))
        log("CAMERA", "  uploaded %s (HTTP %d)" % (name, r.status_code))
        return True
    if r.status_code in RETRYABLE or r.status_code >= 500:
        log("CAMERA", "  NOT uploaded (HTTP %d, endpoint not ready?). Kept in pending/" % r.status_code)
        return False
    # 400, 401, 413 and similar: retrying will not help
    shutil.move(path, os.path.join(FAILED_DIR, name))
    log("CAMERA", "  REJECTED (HTTP %d): %s. Moved to failed/" % (r.status_code, r.text[:200]))
    return True


def retry_pending():
    files = sorted(f for f in os.listdir(PENDING_DIR) if f.endswith(".jpg"))
    if not files:
        return
    log("CAMERA", "Retrying %d pending photo(s)..." % len(files))
    for name in files:
        if not upload(os.path.join(PENDING_DIR, name)):
            break  # API still down; try the rest next round


def handle_trigger(camera, node_id, source, payload):
    if not UUID_RE.match(node_id):
        log("CAMERA", "%s skipped: node_id is not a UUID" % node_id)
        return
    log("CAMERA", "TRIGGER from %s: %s" % (node_id, json.dumps(payload)))
    if camera is None:
        log("CAMERA", "  no photo: camera is off")
        return
    captured_at = utc_stamp()   # laptop clock, set when the photo is taken
    try:
        data = camera.take_jpeg()
    except RuntimeError as e:
        log("CAMERA", "  CAMERA ERROR: %s" % e)
        return
    path = save_photo(PENDING_DIR, node_id, captured_at, source, data)
    log("CAMERA", "  photo %s (%d KB)" % (os.path.basename(path), len(data) // 1024))
    upload(path)


# ================================================================ MQTT
telemetry_q = queue.Queue()
trigger_q = queue.Queue()


def on_connect(client, userdata, flags, rc, *args):
    rc_value = getattr(rc, "value", rc)
    if rc_value == 0:
        log("MQTT", "connected to %s, listening on %s and %s (mode: %s)"
            % (BROKER, TELEMETRY_TOPIC, TRIGGER_TOPIC, MODE))
        client.subscribe([(TELEMETRY_TOPIC, 1), (TRIGGER_TOPIC, 1)])
    else:
        log("MQTT", "connect failed, rc=%s" % rc)


def on_disconnect(client, userdata, *args):
    log("MQTT", "disconnected, reconnecting...")


def on_message(client, userdata, msg):
    # Only sort the message here. The real work happens in other threads,
    # so the MQTT connection never stalls.
    parts = msg.topic.split("/")
    if len(parts) != 3 or parts[0] != "nodes":
        return
    node_id, kind = parts[1], parts[2]
    payload = msg.payload.decode("utf-8", "replace")

    if kind == "telemetry":
        telemetry_q.put((node_id, payload))
    elif kind == "trigger":
        try:
            data = json.loads(payload)
        except ValueError:
            log("CAMERA", "%s ignored bad trigger payload: %s" % (node_id, payload))
            return
        if not isinstance(data, dict):
            data = {}
        trigger_q.put((node_id, str(data.get("source", "unknown")), data))


def make_client():
    client_id = "gateway-" + uuid.uuid4().hex[:8]
    if hasattr(mqtt, "CallbackAPIVersion"):  # paho-mqtt 2.x
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    else:                                     # paho-mqtt 1.x
        client = mqtt.Client(client_id=client_id)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


# ================================================================ Main
def open_camera():
    try:
        cam = Camera(CAMERA_INDEX)
        log("CAMERA", "ready (index %d)" % CAMERA_INDEX)
        return cam
    except ImportError:
        log("CAMERA", "OFF: OpenCV missing. Run: python -m pip install opencv-python")
    except RuntimeError as e:
        log("CAMERA", "OFF: %s" % e)
    log("CAMERA", "Telemetry still runs. Triggers are logged without photos.")
    return None


def main():
    for d in (PENDING_DIR, UPLOADED_DIR, FAILED_DIR, TEST_DIR):
        os.makedirs(d, exist_ok=True)

    if "--test" in sys.argv:
        cam = open_camera()
        if cam:
            data = cam.take_jpeg()
            path = save_photo(TEST_DIR, "test", utc_stamp(), "test", data)
            log("CAMERA", "test photo saved: %s (%d KB)" % (path, len(data) // 1024))
            cam.close()
        return

    if MODE == "supabase" and not SUPABASE_SERVICE_KEY:
        print("Missing SUPABASE_SERVICE_KEY. Create bridge_secrets.py (see top of this file).")
        return
    if MODE == "api" and not SENSOR_KEY:
        print("Missing SENSOR_KEY. Create bridge_secrets.py (see top of this file).")
        return
    if not SENSOR_KEY:
        log("CAMERA", "WARNING: SENSOR_KEY missing in bridge_secrets.py; image uploads have no key")

    camera = None if "--no-camera" in sys.argv else open_camera()

    threading.Thread(target=telemetry_worker, args=(telemetry_q,), daemon=True).start()

    client = make_client()
    client.connect_async(BROKER, PORT, keepalive=30)
    client.loop_start()

    # The main thread runs the camera, so a slow photo never delays telemetry.
    last_photo = {}         # node_id -> time of last photo
    last_retry = time.time()
    log("MAIN", "running. Ctrl+C to stop.")
    try:
        while True:
            try:
                node_id, source, payload = trigger_q.get(timeout=1)
            except queue.Empty:
                node_id = None

            if node_id:
                if time.time() - last_photo.get(node_id, 0) < COOLDOWN_S:
                    log("CAMERA", "trigger from %s (%s) skipped: cooldown" % (node_id, source))
                else:
                    last_photo[node_id] = time.time()
                    try:
                        handle_trigger(camera, node_id, source, payload)
                    except Exception as e:   # camera problems never stop the bridge
                        log("CAMERA", "ERROR: %s" % e)

            if camera and time.time() - last_retry >= RETRY_EVERY_S:
                last_retry = time.time()
                try:
                    retry_pending()
                except Exception as e:
                    log("CAMERA", "retry ERROR: %s" % e)
    except KeyboardInterrupt:
        log("MAIN", "stopping...")
    finally:
        client.loop_stop()
        client.disconnect()
        if camera:
            camera.close()


if __name__ == "__main__":
    main()