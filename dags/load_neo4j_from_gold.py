from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="load_neo4j_from_gold",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # déclenché par le TriggerDagRunOperator du DAG ingest_kg
    catchup=False,
    default_args={"retries": 0},
    tags=["kg", "neo4j"],
    max_active_runs=1,
    concurrency=1,
) as dag:

    load = BashOperator(
        task_id="load_neo4j",
        bash_command="NEO4J_URI=bolt://kg_neo4j:7687 python /opt/airflow/scripts/load_neo4j_from_gold.py",
    )
