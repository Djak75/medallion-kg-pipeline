from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

GOLD_DIR = "/opt/airflow/data/gold"

with DAG(
    dag_id="load_neo4j_from_gold",
    start_date=datetime(2025, 10, 23),
    schedule=None,          # déclenché par le TriggerDagRunOperator
    catchup=False,
    default_args={"retries": 0},
    tags=["kg", "neo4j"],
) as dag:

    # 1) Attendre que "export_gold_csv" du DAG "ingest_kg" soit en SUCCESS
    # wait_gold = ExternalTaskSensor(
    #     task_id="wait_for_export_gold",
    #     external_dag_id="ingest_kg",
    #     external_task_id="export_gold_csv",
    #     allowed_states=["success"],
    #     failed_states=["failed", "skipped"],
    #     mode="reschedule",       
    #     poke_interval=30,         
    #     timeout=60 * 60 * 6,      # 6h max d'attente 
    # )

    # 2) Charger dans Neo4j depuis GOLD
    load = BashOperator(
        task_id="load_neo4j",
        bash_command=(
            "python /opt/airflow/scripts/load_neo4j_from_gold.py "
            "--uri bolt://kg_neo4j:7687 "
            f"--nodes {GOLD_DIR}/nodes.csv "
            f"--edges {GOLD_DIR}/edges.csv "
            "--batch 20000"
        ),
    )
    
    # wait_gold >> load