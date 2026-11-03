"""Simulateur de capteurs IoT -> publication MQTT.

Topics : iot/<salle>/<type_capteur>
Injecte ~3 % de valeurs manquantes ou aberrantes pour justifier le nettoyage.
"""
import json
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from config import MQTT_HOST, MQTT_PORT, MQTT_TOPIC_ROOT

SENSORS = [
    {"id": "temp-s1", "room": "salle1", "type": "temperature", "unit": "C",   "base": 22,  "noise": 1.5},
    {"id": "hum-s1",  "room": "salle1", "type": "humidity",    "unit": "%",   "base": 45,  "noise": 5},
    {"id": "co2-s1",  "room": "salle1", "type": "co2",         "unit": "ppm", "base": 600, "noise": 80},
    {"id": "temp-s2", "room": "salle2", "type": "temperature", "unit": "C",   "base": 19,  "noise": 1.0},
    {"id": "hum-s2",  "room": "salle2", "type": "humidity",    "unit": "%",   "base": 55,  "noise": 4},
]


def read_value(s):
    r = random.random()
    if r < 0.015:
        return None                                  # valeur manquante
    if r < 0.03:
        return s["base"] * random.choice([-5, 10])   # valeur aberrante
    return round(random.gauss(s["base"], s["noise"]), 2)


def main(interval=1.0):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="iot-publisher")
    client.connect(MQTT_HOST, MQTT_PORT)
    client.loop_start()
    print(f"Publication sur {MQTT_HOST}:{MQTT_PORT} (Ctrl+C pour arrêter)")
    try:
        while True:
            for s in SENSORS:
                msg = {
                    "sensor_id": s["id"],
                    "room": s["room"],
                    "type": s["type"],
                    "value": read_value(s),
                    "unit": s["unit"],
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
                topic = f"{MQTT_TOPIC_ROOT}/{s['room']}/{s['type']}"
                client.publish(topic, json.dumps(msg), qos=1)
                print(topic, msg["value"])
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()