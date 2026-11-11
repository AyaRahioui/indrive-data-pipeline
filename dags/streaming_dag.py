import json
import os
import random
import uuid
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from db import get_conn

DATA_FILE = "/opt/airflow/data/mockaroo_rides.json"

def fetch_events(n=5):
    key = os.getenv("MOCKAROO_KEY")
    if key:
        try:
            r = requests.get(
                "https://api.mockaroo.com/api/generate.json",
                params={"key": key, "schema": "indrive_rides", "count": n},
                timeout=10,
                verify=os.getenv("VERIFY_SSL", "true") == "true",
            )
            r.raise_for_status()
            events = r.json()
            print(f"SOURCE=API Mockaroo ({len(events)} événements)")
            return events
        except Exception as e:
            print(f"API Mockaroo indisponible ({e}), repli sur le fichier")

    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            print("SOURCE=fichier local")
            return random.sample(json.load(f), n)

    print("SOURCE=données aléatoires")
    return [{
        "driver_id": random.randint(1, 8),
        "pickup_zone_id": random.randint(1, 8),
        "dropoff_zone_id": random.randint(1, 8),
        "distance_km": round(random.uniform(1, 30), 1),
        "fare_amount": round(random.uniform(-5, 120), 2),
    } for _ in range(n)]

def to_ride(e):
    """Transforme un événement brut en une ligne de raw.rides."""
    # ~5 % de chauffeurs qui n'existent pas dans drivers.csv
    driver_id = 999 if random.random() < 0.05 else int(e["driver_id"])
    return (
        str(uuid.uuid4()),
        driver_id,
        int(e["pickup_zone_id"]),
        int(e["dropoff_zone_id"]),
        datetime.utcnow(),
        e["distance_km"],
        e["fare_amount"],
    )

def load_rides():
    rows = [to_ride(e) for e in fetch_events()]
    if random.random() < 0.03:        # doublon volontaire, pour tester le staging
        rows.append(rows[0])
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO raw.rides
                   (ride_id, driver_id, pickup_zone_id, dropoff_zone_id,
                    requested_at, distance_km, fare_amount)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                rows,
            )
    finally:
        conn.close()

with DAG(
    dag_id="1_streaming",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
) as dag:
    PythonOperator(task_id="load_rides", python_callable=load_rides)