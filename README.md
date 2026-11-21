# Data Lake IoT dans le cloud

Collecte de données IoT simulées, stockage des données brutes et traitées dans un data lake MinIO (S3), visualisation avec Jupyter et réplication vers un stockage cloud (Backblaze B2).

## Architecture
```
publisher.py --MQTT--> Mosquitto --> subscriber.py --> MinIO raw/ --> transform.py --> MinIO processed/ --> Jupyter
                                                                                            |
                                                                              sync_cloud.py --> Backblaze B2
```

## Organisation du data lake
```
raw/type=<capteur>/date=YYYY-MM-DD/hour=HH/batch_<ts>.jsonl   # brut (JSON Lines)
processed/measures/date=YYYY-MM-DD/measures.parquet            # nettoyé
processed/agg_1min/date=YYYY-MM-DD/agg_1min.parquet            # agrégé par minute
```

## Lancement
```powershell
docker compose up -d                       # Mosquitto + MinIO (console : http://localhost:9001)
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python subscriber.py                       # terminal 1 : ingestion
python publisher.py                        # terminal 2 : capteurs simulés
python transform.py                        # raw -> processed
python sync_cloud.py                       # processed -> cloud (nécessite .env)
```
Visualisation : `notebooks/analyse.ipynb`.

## Configuration cloud
Copier `.env.example` en `.env` et renseigner les identifiants Backblaze B2 (non versionné).