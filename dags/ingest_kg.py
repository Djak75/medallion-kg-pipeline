from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="ingest_kg",
    start_date=datetime(2025, 1, 1),
    schedule=None,            # On exécute manuellement
    catchup=False,
    default_args={"retries": 0},
    tags=["medallion", "kg"],
    max_active_runs=1,        # Pour éviter les chevauchements
    concurrency=1,
) as dag:

    # Bronze: CSV -> Parquet
    bronze = BashOperator(
        task_id="bronze_to_parquet",
        bash_command="python /opt/airflow/scripts/to_parquet.py",
    )

    # GX: contrôle qualité sur Bronze
    gx = BashOperator(
        task_id="gx_checkpoint",
        bash_command="python /opt/airflow/quality/gx_checkpoint.py",
    )

    # Silver: partitionnement des edges
    silver = BashOperator(
        task_id="partition_edges",
        bash_command="python /opt/airflow/scripts/partition_edges.py",
    )

    # Gold: export CSV depuis Silver
    gold = BashOperator(
        task_id="export_gold_csv",
        bash_command="python /opt/airflow/scripts/export_gold_csv.py",
    )

    # Déclenchement du DAG de chargement dans Neo4j
    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load_neo4j_from_gold",
        trigger_dag_id="load_neo4j_from_gold",
        wait_for_completion=False,
    )

    # Orchestration des tâches
    bronze >> gx >> silver >> gold >> trigger_load
