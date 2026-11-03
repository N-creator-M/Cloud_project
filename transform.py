"""Traitement : zone RAW -> zone PROCESSED (Parquet).

1. Lit tous les fichiers JSON Lines bruts d'une date
2. Nettoie : valeurs manquantes, hors plage physique, doublons
3. Écrit les mesures nettoyées + un agrégat par minute en Parquet

Usage : python transform.py             (date du jour, UTC)
        python transform.py 2026-11-03
"""
import io
import json
import sys
from datetime import datetime, timezone

import pandas as pd

from config import BUCKET, PROCESSED_PREFIX, RAW_PREFIX, get_client

# Plages physiquement plausibles par type de capteur
VALID_RANGES = {
    "temperature": (-10, 50),
    "humidity": (0, 100),
    "co2": (300, 5000),
}

s3 = get_client()


def read_raw(date):
    """Lit tous les objets raw/ de la date donnée et les met dans un DataFrame."""
    rows = []
    for obj in s3.list_objects(BUCKET, prefix=f"{RAW_PREFIX}/", recursive=True):
        if f"date={date}/" not in obj.object_name:
            continue
        resp = s3.get_object(BUCKET, obj.object_name)
        try:
            for line in resp.read().decode().splitlines():
                if line.strip():
                    rows.append(json.loads(line))
        finally:
            resp.close()
            resp.release_conn()
    return pd.DataFrame(rows)


def clean(df):
    n0 = len(df)
    df = df[df["type"].isin(VALID_RANGES)].copy()          # types connus seulement
    df = df.dropna(subset=["value"])                        # valeurs manquantes
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    low = df["type"].map(lambda t: VALID_RANGES[t][0])
    high = df["type"].map(lambda t: VALID_RANGES[t][1])
    df = df[(df["value"] >= low) & (df["value"] <= high)]   # valeurs aberrantes
    df = df.drop_duplicates(subset=["sensor_id", "ts"])     # doublons (QoS 1)
    print(f"Nettoyage : {n0} lignes brutes -> {len(df)} valides ({n0 - len(df)} rejetées)")
    return df.drop(columns=["topic"], errors="ignore")


def aggregate(df):
    """Moyenne / min / max / nombre de mesures par capteur et par minute."""
    return (df.set_index("ts")
              .groupby(["sensor_id", "room", "type", "unit"])["value"]
              .resample("1min").agg(["mean", "min", "max", "count"])
              .reset_index()
              .dropna(subset=["mean"]))


def write_parquet(df, key):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(BUCKET, key, buf, buf.getbuffer().nbytes,
                  content_type="application/octet-stream")
    print(f"[PROCESSED] {len(df)} lignes -> {key}")


def main():
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw = read_raw(date)
    if raw.empty:
        print(f"Aucune donnée brute pour {date}")
        return
    cleaned = clean(raw)
    write_parquet(cleaned, f"{PROCESSED_PREFIX}/measures/date={date}/measures.parquet")
    write_parquet(aggregate(cleaned), f"{PROCESSED_PREFIX}/agg_1min/date={date}/agg_1min.parquet")


if __name__ == "__main__":
    main()