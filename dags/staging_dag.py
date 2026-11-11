from datetime import datetime
from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.state import DagRunState
from db import run_sql_file

def ran_once(dag_id):
    """Vrai si ce DAG a déjà réussi au moins une fois."""
    return len(DagRun.find(dag_id=dag_id, state=DagRunState.SUCCESS)) > 0

with DAG(
    dag_id="3_staging",
    start_date=datetime(2024, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    wait_sources = PythonSensor(
        task_id="wait_sources",
        python_callable=lambda: ran_once("1_streaming") and ran_once("2_batch"),
        poke_interval=30,
        mode="reschedule",
    )

    stg_tasks = [
        PythonOperator(
            task_id=f"stg_{t}",
            python_callable=run_sql_file,
            op_args=[f"/opt/airflow/sql/staging/{t}.sql"],
        )
        for t in ["drivers", "vehicles", "zones", "rides"]
    ]

    trigger_marts = TriggerDagRunOperator(
        task_id="trigger_marts",
        trigger_dag_id="4_marts",
    )

    wait_sources >> stg_tasks >> trigger_marts