"""Configuration centralisée (surchargeable par variables d'environnement).
Pour passer au cloud : changer S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY / S3_SECURE.
"""
import os

# MQTT
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC_ROOT = os.getenv("MQTT_TOPIC_ROOT", "iot")

# Object Storage (MinIO local par défaut)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_SECURE = os.getenv("S3_SECURE", "false").lower() == "true"
BUCKET = os.getenv("BUCKET", "iot-datalake")

# Zones du data lake
RAW_PREFIX = "raw"
PROCESSED_PREFIX = "processed"

# Ingestion : écriture tous les N messages ou toutes les T secondes
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BATCH_SECONDS = int(os.getenv("BATCH_SECONDS", "30"))


def get_client():
    from minio import Minio
    return Minio(S3_ENDPOINT, access_key=S3_ACCESS_KEY,
                 secret_key=S3_SECRET_KEY, secure=S3_SECURE)