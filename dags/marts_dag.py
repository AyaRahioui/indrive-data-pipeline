from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from db import run_sql_file

def mart_task(name):
    return PythonOperator(
        task_id=f"mart_{name}",
        python_callable=run_sql_file,
        op_args=[f"/opt/airflow/sql/mart/{name}.sql"],
    )

with DAG(
    dag_id="4_marts",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # démarré par 3_staging
    catchup=False,
) as dag:
    rides = mart_task("rides")
    zone = mart_task("revenue_by_zone")
    leaderboard = mart_task("driver_leaderboard")

    rides >> [zone, leaderboard]