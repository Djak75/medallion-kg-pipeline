from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

BRONZE_IN = "/opt/airflow/data/raw"
BRONZE_OUT = "/opt/airflow/data/bronze"
SILVER_OUT = "/opt/airflow/data/silver"
GOLD_OUT = "/opt/airflow/data/gold"

with DAG(
    dag_id="ingest_kg",
    start_date=datetime(2025, 1, 1),
    schedule=None,            
    catchup=False,
    default_args={"retries": 0},
    tags=["medallion", "kg"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_to_parquet",
        bash_command=f"python /opt/airflow/scripts/to_parquet.py --in {BRONZE_IN} --out {BRONZE_OUT}",
    )

    gx = BashOperator(
        task_id="gx_checkpoint",
        bash_command=f"python /opt/airflow/quality/gx_checkpoint.py --nodes {BRONZE_OUT}/nodes.parquet --edges {BRONZE_OUT}/edges.parquet",
    )

    silver = BashOperator(
        task_id="partition_edges",
        bash_command=f"python /opt/airflow/scripts/partition_edges.py --in {BRONZE_OUT} --out {SILVER_OUT} --partitions 8",
    )

    gold = BashOperator(
        task_id="export_gold_csv",
        bash_command=f"python /opt/airflow/scripts/export_gold_csv.py --silver {SILVER_OUT} --gold {GOLD_OUT} --partitions 8",
    )

    trigger_load = TriggerDagRunOperator(
    task_id="trigger_load_neo4j_from_gold",
    trigger_dag_id="load_neo4j_from_gold", 
    wait_for_completion=False,               
    )

    bronze >> gx >> silver >> gold >> trigger_load
