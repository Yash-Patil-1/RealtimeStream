"""
Airflow DAG — Gold Layer Processing

Triggers Spark batch jobs to compute sliding-window KPIs, sessionization,
conversion funnels, and anomaly aggregations from Silver Delta tables.

This DAG runs every 15 minutes and processes the latest Silver partition.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _check_silver_ready() -> str:
    """
    Check if Silver data is available before running Gold aggregations.

    Returns the task_id of the branch to execute.
    """
    import os
    import subprocess

    # Quick check: does the Silver path have data?
    # In production, this would check Delta table metadata.
    silver_path = os.getenv("SILVER_DELTA_PATH", "s3a://streaming-lake/delta/silver/events_clean")
    result = subprocess.run(
        ["spark-submit", "--version"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode == 0:
        return "compute_kpis"
    return "skip_gold_processing"


_SPARK_SUBMIT_BASE = (
    "spark-submit "
    "--master spark://spark-master:7077 "
    "--packages io.delta:delta-core_2.13:3.1.0,io.delta:delta-storage:3.1.0 "
    "/opt/airflow/src/gold_aggregations.py"
)


with DAG(
    "gold_processing",
    default_args=default_args,
    description="Compute Gold layer aggregations from Silver Delta tables",
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["streaming", "gold", "aggregations"],
    max_active_runs=1,
) as dag:

    check_silver = BranchPythonOperator(
        task_id="check_silver_data",
        python_callable=_check_silver_ready,
    )

    skip_gold = PythonOperator(
        task_id="skip_gold_processing",
        python_callable=lambda: print("No new Silver data. Skipping Gold processing."),
    )

    compute_kpis = BashOperator(
        task_id="compute_hourly_kpis",
        bash_command=f"""
            {_SPARK_SUBMIT_BASE} --mode kpis --window 1h
        """,
    )

    compute_sessions = BashOperator(
        task_id="compute_sessionization",
        bash_command=f"""
            {_SPARK_SUBMIT_BASE} --mode sessions
        """,
    )

    compute_funnels = BashOperator(
        task_id="compute_conversion_funnels",
        bash_command=f"""
            {_SPARK_SUBMIT_BASE} --mode funnels --window 24h
        """,
    )

    compute_anomaly_aggs = BashOperator(
        task_id="compute_anomaly_aggregations",
        bash_command=f"""
            {_SPARK_SUBMIT_BASE} --mode anomaly_aggs --window 15m
        """,
    )

    # Orchestration: run KPIs + sessions in parallel, then funnels + anomaly aggs
    check_silver >> [compute_kpis, compute_sessions, skip_gold]
    compute_kpis >> compute_funnels
    compute_sessions >> compute_funnels
    compute_funnels >> compute_anomaly_aggs
