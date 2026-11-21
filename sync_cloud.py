"""Réplication du data lake local (MinIO) vers le stockage cloud (Backblaze B2, API S3).

Usage : python sync_cloud.py            (zone processed/ seulement)
        python sync_cloud.py --all      (tout le data lake : raw/ + processed/)
Les identifiants cloud sont lus dans .env (jamais versionné).
"""
import os
import sys

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

from config import BUCKET, PROCESSED_PREFIX, get_client

load_dotenv()

local = get_client()
cloud = Minio(os.environ["CLOUD_ENDPOINT"],
              access_key=os.environ["CLOUD_ACCESS_KEY"],
              secret_key=os.environ["CLOUD_SECRET_KEY"],
              region=os.environ.get("CLOUD_REGION"),
              secure=True)
CLOUD_BUCKET = os.environ["CLOUD_BUCKET"]


def already_in_cloud(key, size):
    """N'envoie pas un objet déjà présent et identique (économie de quota)."""
    try:
        return cloud.stat_object(CLOUD_BUCKET, key).size == size
    except S3Error:
        return False


def main():
    prefix = "" if "--all" in sys.argv else f"{PROCESSED_PREFIX}/"
    sent = skipped = 0
    for obj in local.list_objects(BUCKET, prefix=prefix, recursive=True):
        if already_in_cloud(obj.object_name, obj.size):
            skipped += 1
            continue
        resp = local.get_object(BUCKET, obj.object_name)
        try:
            cloud.put_object(CLOUD_BUCKET, obj.object_name, resp, obj.size)
        finally:
            resp.close()
            resp.release_conn()
        sent += 1
        print(f"[CLOUD] {obj.object_name} ({obj.size} octets)")
    print(f"Terminé : {sent} envoyé(s), {skipped} déjà à jour.")


if __name__ == "__main__":
    main()