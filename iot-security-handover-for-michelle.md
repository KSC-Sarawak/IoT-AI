# IoT Security Handover: Current Design, Gaps, and Broker Options

**From:** IoT owner (sensor node + node_gateway.py)
**To:** Michelle (cybersecurity) and her Claude
**Re:** Findings F-14 and F-15, and the plan to move to a private Mosquitto broker
**Date:** 5 Nov 2026

The fixes and the goal are good. We need to agree on the scope before tomorrow's 5 pm session. Moving to a self-hosted broker is a bigger change than it looks. This note explains the current trust model, what it protects and what it does not, and a lighter option that gives the same TLS and login result.

---

## 1. System as it runs today

```
ESP32 (wiretrap.ino)
  |  MQTT, plain text, port 1883
  v
broker.hivemq.com  (public, shared, no auth)
  |
  v
node_gateway.py  (on the laptop, long-lived process)
  |-- nodes/{node_id}/telemetry -> Supabase "Daun Sense" telemetry table (service role)
  '-- nodes/{node_id}/trigger   -> laptop webcam photo -> captures/pending/
```

- **Hardware:** ESP32 with DHT11, YL-69, HC-SR04, joystick tripwire (GPIO35/32) and LED (GPIO5). No GPS, no camera.
- **Telemetry payload:** matches `TelemetryReadingInput` in the API contract: `ts, temperature, humidity, soil_moisture, distance_cm, motion_detected`.
- **Trigger payload:** `{"ts":"...Z","source":"tripwire"|"motion","deflection":1520}`. Team decision: this is not telemetry and is not in the contract.
- **Why the gateway runs on the laptop:** Supabase Edge Functions cannot hold an MQTT subscription, and the webcam is on the laptop.

## 2. Current trust model

| Secret | Where it lives | Who can use it |
|---|---|---|
| Wi-Fi password, `NODE_ID`, shared node key | `secrets.h` on the ESP32 (gitignored) | The node |
| Shared node key (matching copy) | Gateway config on the laptop | node_gateway.py |
| Supabase service role key | `bridge_secrets.py` on the laptop only (gitignored) | node_gateway.py |

How it works:
1. The node sends the shared key with each message.
2. node_gateway.py drops any message whose key does not match.
3. Only messages that pass are written to Supabase.
4. Only the service role can insert telemetry (RLS). **The Supabase key never touches the ESP32.**

**What this protects:** a random publisher on the public broker cannot write to our database without the key. If an ESP32 is stolen from the forest, it holds no database credentials.

## 3. Known gaps (F-14 and F-15 are valid)

The node-to-broker leg is plain text on a public broker. So:

- Anyone can subscribe to `nodes/#` on broker.hivemq.com and read every message, **including the shared key**.
- With the key, they can forge or replay readings and triggers.
- They can also see when the tripwire fires. For an anti-poaching device, that is a real leak.
- `node_id` is in the topic, so the registered-ID check in the new gateway filters out noise. It does not authenticate the sender.

So the shared key stops casual misuse. It is not a secret on its own. TLS plus broker login is the right fix.

## 4. Michelle's gateway changes: accepted

These stay whichever broker we choose:
- Only registered node IDs accepted (`REGISTERED_NODES`)
- Trigger `source` must be `motion` or `tripwire`
- At most 6 photos per minute in total
- Out-of-range readings dropped (e.g. `temperature` 9999, `humidity` or `soil_moisture` outside 0 to 100, negative `distance_cm`)
- Supabase key stays in `bridge_secrets.py`, `MODE = "supabase"` until `POST /telemetry` stops returning 501

Firmware side: soil moisture will be clamped to 0 to 100, and NaN DHT11 readings will be omitted, so valid readings are not dropped by the new range check.

## 5. Why a self-hosted Mosquitto on the laptop is a big change

| Area | What it involves |
|---|---|
| Single point of failure | The laptop becomes broker + gateway + camera. If it sleeps or moves, everything stops. |
| Network | The ESP32 only reaches the broker on the same LAN (laptop hotspot `DESKTOP-FCAD7GL`, 2.4 GHz). Hotspot IP can change. |
| Certificates | Need our own CA, plus a server cert whose SAN includes the hotspot IP (and `localhost` for the gateway). If the IP changes, the cert breaks. |
| Windows setup | Install Mosquitto as a service, password file, per-node ACLs, firewall rule for inbound 8883. |
| Firmware | `WiFiClientSecure`, CA cert in `secrets.h`, NTP must sync before connect, slower TLS reconnect. Reflash and retest. |
| Per-node credentials | Overlaps CTIP-48 (node registration issues MQTT credentials), which has another owner. |
| Retest | Full telemetry "saved" check and tripwire to photo check. |
| Rework later | When the bridge moves to `services/api`, the broker has to move again anyway. |

This is all doable. It just costs more than one evening, and it adds demo risk.

## 6. Options

### Option A: HiveMQ Cloud Serverless, free tier (recommended for now)
- Managed broker with TLS on port 8883 and username/password logins created in the console. Free plan: 100 connections, 10 GB/month, basic authorization rules, no credit card.
- Fixes F-14/F-15 on the wire: encrypted traffic, no anonymous publish or subscribe.
- No certificates to generate, no laptop broker, no firewall changes, works from any network.
- One login per node (username = node UUID), plus a separate login for the gateway.
- **Firmware change:** `WiFiClientSecure` + `setCACert(<HiveMQ root CA>)` + host, port 8883, user, password.
- **Gateway change:** `tls_set()` + `username_pw_set()` + new host and port.
- **Trade-off:** third-party hosted, no uptime SLA. Fine for a student demo.

### Option B: self-hosted Mosquitto (the CTIP-60 target)
- Matches CTIP-60 as written (`infra/mosquitto/`).
- Best done when the bridge moves to `services/api` on a proper host, not on the demo laptop.

### Option C: minimum, if neither is ready in time
- Stay on the public broker. Replace the plain shared key with an HMAC-SHA256 over the payload, including `ts`. Reject stale timestamps.
- Stops forgery and replay. The key never travels on the wire.
- Does **not** hide readings or trigger timing. Weakest option.

## 7. Proposal for tomorrow (5 pm to 9 pm)

1. Agree on Option A now and keep Option B as the CTIP-60 end state.
2. Michelle creates the HiveMQ Cloud cluster and the logins (one per node, one for the gateway) and shares them privately, not in chat or Git.
3. IoT owner updates firmware and gateway connection code.
4. Test: readings still say "saved", and a trigger still takes a photo.
5. Note in Jira: CTIP-60 TLS + per-node login done via managed broker, Mosquitto self-hosting deferred to `services/api`. Tell the CTIP-48 owner.

## 8. Questions for Michelle

1. Does Option A close F-14 and F-15 for the assessment, or does the brief require self-hosting?
2. Should the gateway still check the shared node key after TLS + broker login, as defence in depth?
3. Who owns creating and rotating MQTT credentials: security (you) or CTIP-48?
4. Are we keeping public broker topics for anything else, or can we drop broker.hivemq.com entirely?
