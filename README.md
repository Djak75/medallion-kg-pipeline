# medallion-kg-pipeline

Pipeline Medaillon (Bronze/Silver/Gold) pour Knowledge Graph à grande échelle : Airflow, OpenLineage/Marquez, Neo4j, FastAPI, Prometheus, Grafana.


# Architecture Médaillon x Graphes de connaissances

Pipeline complet Bronze → Silver → Gold pour un Knowledge Graph à grande échelle.

## Objectif

- Structurer un projet data moderne en couches (Bronze → Silver → Gold)
- Orchestrer avec Apache Airflow
- Tracer la lignée avec OpenLineage/Marquez
- Monitorer avec Prometheus + Grafana
- Stocker & requêter en graphe avec Neo4j
- Exposer via une API FastAPI

## Stack & ports

- Neo4j (7474 UI, 7687 Bolt)
- Airflow (8080)
- Marquez (5000)
- Prometheus (9090)
- Grafana (3000)
- FastAPI (8000)

## Layout

projet/
├── docker-compose.yaml
├── .env
├── Makefile
├── README.md
├── api/
├── dags/
├── data/{raw,bronze,silver,gold}
├── scripts/
├── quality/
├── schema/
├── lineage/
└── grafana/
