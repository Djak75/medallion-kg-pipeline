# Démarre tous les services (api, neo4j, postgres, airflow)
up:
	docker compose up -d

# Stop les services (sans supprimer les volumes)
down:
	docker compose down

# Lance webserver + scheduler Airflow
airflow:
	docker compose up -d airflow airflow-scheduler

# Lance Neo4j
neo4j:
	docker compose up -d neo4j

# Génère des CSV bruts dans data/raw/
seed:
	python scripts/generate_sample_data.py

# CSV -> Parquet (Bronze)
bronze:
	python scripts/to_parquet.py

# Partition edges (Silver)
silver:
	python scripts/partition_edges.py

# Parquet Silver -> CSV Gold (réécrit nodes/edges)
gold:
	python scripts/export_gold_csv.py


