from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from db import get_conn

def reload_table(table):
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"TRUNCATE raw.{table}")          # vider
            with open(f"/opt/airflow/data/{table}.csv") as f:
                cur.copy_expert(f"COPY raw.{table} FROM STDIN WITH CSV HEADER", f)  # recharger
    finally:
        conn.close()

with DAG(
    dag_id="2_batch",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    for t in ["drivers", "vehicles", "zones"]:
        PythonOperator(task_id=f"load_{t}", python_callable=reload_table, op_args=[t])