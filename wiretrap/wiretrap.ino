/*
 * Rainforest Plant Monitor - ESP32 sensor node
 * Jira: CTIP-17 (readings), CTIP-59 (motion), CTIP-60 (MQTT publishing)
 *
 * Publishes ONE JSON message per reading to:  nodes/{NODE_ID}/telemetry
 * Payload matches TelemetryReadingInput in the OpenAPI contract:
 *   { "ts": "...Z", "temperature": 28.4, "humidity": 91.0,
 *     "soil_moisture": 62, "distance_cm": 45.2, "motion_detected": false }
 *
 * Libraries (Arduino Library Manager):
 *   - "DHT sensor library" by Adafruit
 *   - "PubSubClient" by Nick O'Leary
 *
 * Put WIFI_SSID, WIFI_PASSWORD and NODE_ID in secrets.h (gitignored).
 */
#include <WiFi.h>
#include <PubSubClient.h>
#include <time.h>
#include "DHT.h"
#include "secrets.h"

// ---------- MQTT (public test broker for now; TLS + Mosquitto later, CTIP-60)
const char* MQTT_BROKER = "broker.hivemq.com";
const int   MQTT_PORT   = 1883;

// ---------- Timing
const unsigned long PUBLISH_INTERVAL_MS = 10000; // normal reading interval
const unsigned long DISTANCE_SAMPLE_MS  = 300;   // motion sampling (CTIP-59)
const unsigned long MOTION_MIN_GAP_MS   = 2000;  // fastest publish after motion
const unsigned long WIFI_TIMEOUT_MS     = 20000;
const unsigned long MQTT_RETRY_MS       = 5000;

// ---------- Motion
const float MOTION_THRESHOLD_CM = 15.0;  // change that counts as motion
const float MAX_VALID_CM        = 400.0; // HC-SR04 range limit

// ---------- Pins
#define DHTPIN          4
#define DHTTYPE         DHT11
#define SOIL_POWER_PIN  25
#define SOIL_ANALOG_PIN 34   // ADC1, safe with Wi-Fi on
#define TRIG_PIN        18
#define ECHO_PIN        19   // through a voltage divider (5 V -> 3.3 V)

const int SOIL_DRY_RAW = 3800;
const int SOIL_WET_RAW = 1300;

DHT dht(DHTPIN, DHTTYPE);
WiFiClient espClient;
PubSubClient mqttClient(espClient);

char topic[96];
float lastDistance = -1.0;
bool motionSinceLastPublish = false;
bool ntpStarted = false;
unsigned long lastPublish = 0, lastSample = 0, lastMqttTry = 0;

// ---------------------------------------------------------------- Wi-Fi
void connectWiFi() {
  if (WiFi.status() == WL_CONNECTED) return;

  Serial.printf("WiFi: connecting to \"%s\"", WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  unsigned long start = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - start < WIFI_TIMEOUT_MS) {
    delay(500);
    Serial.print(".");
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.printf("\nWiFi: OK, IP %s, signal %d dBm\n",
                  WiFi.localIP().toString().c_str(), WiFi.RSSI());
    if (!ntpStarted) {
      configTime(0, 0, "pool.ntp.org", "time.google.com"); // UTC
      ntpStarted = true;
    }
  } else {
    Serial.printf("\nWiFi: FAILED (status %d). Check the hotspot is 2.4 GHz "
                  "and the SSID/password are right. Retrying...\n", WiFi.status());
  }
}

// ---------------------------------------------------------------- Time
bool isoNow(char* buf, size_t n) {
  time_t now = time(nullptr);
  if (now < 1700000000) return false; // clock not synced yet
  struct tm t;
  gmtime_r(&now, &t);
  strftime(buf, n, "%Y-%m-%dT%H:%M:%SZ", &t);
  return true;
}

// ---------------------------------------------------------------- MQTT
void connectMQTT() {
  if (mqttClient.connected() || millis() - lastMqttTry < MQTT_RETRY_MS) return;
  lastMqttTry = millis();

  String clientId = "esp32-" + String((uint32_t)ESP.getEfuseMac(), HEX);
  Serial.printf("MQTT: connecting to %s:%d as %s ... ",
                MQTT_BROKER, MQTT_PORT, clientId.c_str());
  if (mqttClient.connect(clientId.c_str())) {
    Serial.println("OK");
  } else {
    // -4 timeout, -2 network failed, 5 not authorised
    Serial.printf("failed, rc=%d\n", mqttClient.state());
  }
}

// ---------------------------------------------------------------- Sensors
int getSoilMoisturePercent() {
  digitalWrite(SOIL_POWER_PIN, HIGH);
  delay(15);
  int raw = analogRead(SOIL_ANALOG_PIN);
  digitalWrite(SOIL_POWER_PIN, LOW);
  return constrain(map(raw, SOIL_DRY_RAW, SOIL_WET_RAW, 0, 100), 0, 100);
}

float getDistanceCm() {
  digitalWrite(TRIG_PIN, LOW);
  delayMicroseconds(2);
  digitalWrite(TRIG_PIN, HIGH);
  delayMicroseconds(10);
  digitalWrite(TRIG_PIN, LOW);
  long duration = pulseIn(ECHO_PIN, HIGH, 30000);
  if (duration == 0) return -1.0;
  float cm = duration * 0.0343 / 2.0;
  return (cm > MAX_VALID_CM) ? -1.0 : cm;
}

void sampleDistance() {
  float d = getDistanceCm();
  if (d < 0) return; // ignore timed-out or out-of-range pings
  if (lastDistance >= 0 && fabs(d - lastDistance) > MOTION_THRESHOLD_CM) {
    motionSinceLastPublish = true;
    Serial.printf("Motion: %.1f -> %.1f cm\n", lastDistance, d);
  }
  lastDistance = d;
}

// ---------------------------------------------------------------- Publish
void publishReading() {
  lastPublish = millis();

  char ts[25];
  if (!isoNow(ts, sizeof(ts))) {
    Serial.println("Skip: clock not synced yet (NTP)");
    return;
  }

  float humidity = dht.readHumidity();
  float temperature = dht.readTemperature();
  int soil = getSoilMoisturePercent();

  char json[256];
  int n = snprintf(json, sizeof(json), "{\"ts\":\"%s\"", ts);
  if (!isnan(temperature)) n += snprintf(json + n, sizeof(json) - n, ",\"temperature\":%.1f", temperature);
  if (!isnan(humidity))    n += snprintf(json + n, sizeof(json) - n, ",\"humidity\":%.1f", humidity);
  n += snprintf(json + n, sizeof(json) - n, ",\"soil_moisture\":%d", soil);
  if (lastDistance >= 0)   n += snprintf(json + n, sizeof(json) - n, ",\"distance_cm\":%.1f", lastDistance);
  snprintf(json + n, sizeof(json) - n, ",\"motion_detected\":%s}",
           motionSinceLastPublish ? "true" : "false");

  if (mqttClient.publish(topic, json)) {
    Serial.printf("SENT %s %s\n", topic, json);
    motionSinceLastPublish = false;
  } else {
    Serial.printf("SEND FAILED %s\n", topic);
  }
}

// ---------------------------------------------------------------- Main
void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("\n=== Rainforest Plant Monitor ===");

  dht.begin();
  pinMode(SOIL_POWER_PIN, OUTPUT);
  digitalWrite(SOIL_POWER_PIN, LOW);
  pinMode(SOIL_ANALOG_PIN, INPUT);
  pinMode(TRIG_PIN, OUTPUT);
  pinMode(ECHO_PIN, INPUT);
  digitalWrite(TRIG_PIN, LOW);

  snprintf(topic, sizeof(topic), "nodes/%s/telemetry", NODE_ID);
  Serial.printf("Topic: %s\n", topic);

  mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
  mqttClient.setBufferSize(512); // default 256 is tight for JSON
  mqttClient.setKeepAlive(30);

  connectWiFi();
}

void loop() {
  connectWiFi();
  if (WiFi.status() != WL_CONNECTED) return;

  connectMQTT();
  mqttClient.loop(); // never block this with long delay()

  unsigned long now = millis();
  if (now - lastSample >= DISTANCE_SAMPLE_MS) {
    lastSample = now;
    sampleDistance();
  }

  bool due = now - lastPublish >= PUBLISH_INTERVAL_MS;
  bool motionNow = motionSinceLastPublish && now - lastPublish >= MOTION_MIN_GAP_MS;
  if (mqttClient.connected() && (due || motionNow)) publishReading();
}
