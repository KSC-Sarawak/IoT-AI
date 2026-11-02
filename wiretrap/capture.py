"""
capture.py - laptop webcam camera for the sensor node (Jira CTIP-62)

Listens on MQTT for camera triggers from the ESP32:
    topic    nodes/{node_id}/trigger
    payload  {"ts": "...Z", "source": "tripwire", "deflection": 1520}
             {"ts": "...Z", "source": "motion"}

On each trigger it takes ONE webcam photo and uploads the raw JPEG
(2 MB maximum) to:
    POST {API_BASE_URL}/sensor-nodes/{node_id}/images
    headers: Content-Type: image/jpeg, X-Sensor-Key, X-Captured-At

Until the API endpoint works, photos wait in pending/ and are retried
every RETRY_EVERY_S seconds. Uploaded photos move to uploaded/.

Run (PowerShell, Python 3):
    .venv\\Scripts\\Activate.ps1
    pip install opencv-python paho-mqtt requests
    python capture.py           # normal mode, waits for the ESP32
    python capture.py --test    # take one photo now, no ESP32 needed

Secrets: SENSOR_KEY is read from bridge_secrets.py (do not commit).
"""
import json
import os
import queue
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone

import cv2
import paho.mqtt.client as mqtt
import requests

# ---------------------------------------------------------------- Settings
MQTT_BROKER = "broker.hivemq.com"   # same broker as the ESP32 and bridge.py
MQTT_PORT = 1883
TRIGGER_TOPIC = "nodes/+/trigger"

API_BASE_URL = "http://localhost:8000"  # change to the team's API address
UPLOAD_TIMEOUT_S = 15
RETRY_EVERY_S = 30

CAMERA_INDEX = 0            # 0 = built-in laptop webcam
FRAME_WIDTH = 1280
FRAME_HEIGHT = 720
JPEG_QUALITY = 85
MAX_IMAGE_BYTES = 2 * 1024 * 1024   # contract limit: 2 MB
COOLDOWN_S = 5              # at most one photo per node every 5 s

TEST_NODE_ID = "test-node"  # used by --test when no ESP32 is involved

HERE = os.path.dirname(os.path.abspath(__file__))
PENDING_DIR = os.path.join(HERE, "captures", "pending")
UPLOADED_DIR = os.path.join(HERE, "captures", "uploaded")
FAILED_DIR = os.path.join(HERE, "captures", "failed")

try:
    from bridge_secrets import SENSOR_KEY
except ImportError:
    SENSOR_KEY = ""
    print("WARNING: SENSOR_KEY not found in bridge_secrets.py; uploads will have no key")


# ---------------------------------------------------------------- Camera
class Camera:
    """Keeps the webcam open so a photo is taken fast after a trigger."""

    def __init__(self, index):
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
        print("Camera: ready (index %d)" % index)

    def take_jpeg(self):
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


# ---------------------------------------------------------------- Files
def utc_stamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def save_pending(node_id, captured_at, source, data):
    """Save the photo first, so it is never lost if the upload fails."""
    safe_ts = captured_at.replace(":", "")  # Windows file names cannot contain ':'
    name = "%s__%s__%s__%s.jpg" % (node_id, safe_ts, source, uuid.uuid4().hex[:6])
    path = os.path.join(PENDING_DIR, name)
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


# ---------------------------------------------------------------- Upload
RETRYABLE = {404, 408, 429, 501, 502, 503, 504}


def upload(path):
    """Returns True if done (uploaded or given up), False to retry later."""
    node_id, captured_at = parse_pending_name(path)
    url = "%s/sensor-nodes/%s/images" % (API_BASE_URL.rstrip("/"), node_id)
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
        print("  NOT uploaded (API unreachable: %s). Kept in pending/" % e.__class__.__name__)
        return False

    name = os.path.basename(path)
    if 200 <= r.status_code < 300:
        shutil.move(path, os.path.join(UPLOADED_DIR, name))
        print("  uploaded %s (HTTP %d)" % (name, r.status_code))
        return True
    if r.status_code in RETRYABLE or r.status_code >= 500:
        print("  NOT uploaded (HTTP %d, endpoint not ready?). Kept in pending/" % r.status_code)
        return False
    # 400, 401, 413 and similar: retrying will not help
    shutil.move(path, os.path.join(FAILED_DIR, name))
    print("  REJECTED (HTTP %d): %s. Moved to failed/" % (r.status_code, r.text[:200]))
    return True


def retry_pending():
    files = sorted(f for f in os.listdir(PENDING_DIR) if f.endswith(".jpg"))
    if not files:
        return
    print("Retrying %d pending photo(s)..." % len(files))
    for name in files:
        if not upload(os.path.join(PENDING_DIR, name)):
            break  # API still down; try the rest next round


# ---------------------------------------------------------------- Capture
def handle_trigger(camera, node_id, source):
    captured_at = utc_stamp()   # laptop clock, set when the photo is taken
    try:
        data = camera.take_jpeg()
    except RuntimeError as e:
        print("  CAMERA ERROR: %s" % e)
        return
    path = save_pending(node_id, captured_at, source, data)
    print("  photo %s (%d KB)" % (os.path.basename(path), len(data) // 1024))
    upload(path)


# ---------------------------------------------------------------- MQTT
triggers = queue.Queue()


def on_connect(client, userdata, flags, rc, *args):
    rc_value = getattr(rc, "value", rc)
    if rc_value == 0:
        print("MQTT: connected, listening on %s" % TRIGGER_TOPIC)
        client.subscribe(TRIGGER_TOPIC, qos=1)
    else:
        print("MQTT: connect failed, rc=%s" % rc)


def on_message(client, userdata, msg):
    # topic: nodes/{node_id}/trigger
    parts = msg.topic.split("/")
    if len(parts) != 3:
        return
    node_id = parts[1]
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        print("Ignored bad trigger payload on %s" % msg.topic)
        return
    source = str(payload.get("source", "unknown"))
    # Hand off to the main thread; never block the MQTT thread with the camera.
    triggers.put((node_id, source, payload))


def make_client():
    client_id = "capture-" + uuid.uuid4().hex[:8]
    if hasattr(mqtt, "CallbackAPIVersion"):  # paho-mqtt 2.x
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    else:                                     # paho-mqtt 1.x
        client = mqtt.Client(client_id=client_id)
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


# ---------------------------------------------------------------- Main
def main():
    for d in (PENDING_DIR, UPLOADED_DIR, FAILED_DIR):
        os.makedirs(d, exist_ok=True)

    camera = Camera(CAMERA_INDEX)

    if "--test" in sys.argv:
        print("Test mode: one photo for node '%s'" % TEST_NODE_ID)
        handle_trigger(camera, TEST_NODE_ID, "test")
        camera.close()
        return

    client = make_client()
    client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=30)
    client.loop_start()

    last_photo = {}         # node_id -> time of last photo
    last_retry = time.time()
    print("Waiting for triggers. Ctrl+C to stop.")
    try:
        while True:
            try:
                node_id, source, payload = triggers.get(timeout=1)
            except queue.Empty:
                node_id = None

            if node_id:
                since = time.time() - last_photo.get(node_id, 0)
                if since < COOLDOWN_S:
                    print("Trigger from %s (%s) skipped: cooldown" % (node_id, source))
                else:
                    last_photo[node_id] = time.time()
                    print("TRIGGER from %s: %s" % (node_id, json.dumps(payload)))
                    handle_trigger(camera, node_id, source)

            if time.time() - last_retry >= RETRY_EVERY_S:
                last_retry = time.time()
                retry_pending()
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        client.loop_stop()
        client.disconnect()
        camera.close()


if __name__ == "__main__":
    main()