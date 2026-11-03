"""Ingestion : MQTT -> zone RAW du data lake (MinIO).

Regroupe les messages par lots (taille ou durée) et les écrit en JSON Lines :
raw/type=temperature/date=2026-11-03/hour=17/batch_<timestamp>.jsonl
"""
import io
import json
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from config import (BATCH_SECONDS, BATCH_SIZE, BUCKET, MQTT_HOST, MQTT_PORT,
                    MQTT_TOPIC_ROOT, RAW_PREFIX, get_client)

s3 = get_client()
buffer = defaultdict(list)   # un tampon par type de capteur
lock = threading.Lock()
last_flush = time.time()


def flush():
    """Écrit le contenu du tampon dans MinIO, un objet par type de capteur."""
    global last_flush
    with lock:
        batches = dict(buffer)
        buffer.clear()
        last_flush = time.time()
    now = datetime.now(timezone.utc)
    for sensor_type, msgs in batches.items():
        if not msgs:
            continue
        key = (f"{RAW_PREFIX}/type={sensor_type}/date={now:%Y-%m-%d}/"
               f"hour={now:%H}/batch_{now:%Y%m%dT%H%M%S%f}.jsonl")
        data = "\n".join(json.dumps(m) for m in msgs).encode()
        s3.put_object(BUCKET, key, io.BytesIO(data), len(data),
                      content_type="application/x-ndjson")
        print(f"[RAW] {len(msgs):>3} messages -> {key}")


def on_connect(client, userdata, flags, rc, properties=None):
    client.subscribe(f"{MQTT_TOPIC_ROOT}/#", qos=1)
    print(f"Abonné à {MQTT_TOPIC_ROOT}/#")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload)
    except json.JSONDecodeError:
        payload = {"raw": msg.payload.decode(errors="replace"), "type": "invalid"}
    payload["topic"] = msg.topic
    with lock:
        buffer[payload.get("type", "unknown")].append(payload)
        total = sum(len(v) for v in buffer.values())
    if total >= BATCH_SIZE:
        flush()


def timer_loop():
    """Force l'écriture si BATCH_SECONDS sont écoulées (peu de messages)."""
    while True:
        time.sleep(1)
        if time.time() - last_flush >= BATCH_SECONDS:
            flush()


def main():
    threading.Thread(target=timer_loop, daemon=True).start()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="iot-ingestor")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        flush()   # écrire le dernier lot avant de quitter


if __name__ == "__main__":
    main()