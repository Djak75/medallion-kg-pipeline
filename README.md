# Medallion KG Pipeline

Ce projet implémente un pipeline de données de type **médaillon** (Bronze → Silver → Gold) pour un **Knowledge Graph** (KG) avec orchestration via **Apache Airflow** et stockage final dans **Neo4j**.

L’objectif est de générer des données synthétiques, de les transformer progressivement, d’appliquer des contrôles de qualité, puis de les charger dans Neo4j pour visualisation et requêtes graphes.


## Architecture Médaillon

- **Bronze** : stockage brut en **parquet** à partir de CSV (conversion simple).
- **Silver** : partitionnement des arêtes (`edges`) en shards pour équilibrer la distribution.
- **Gold** : export final en CSV (format compatible Neo4j).
- **Orchestration** : DAGs Airflow automatisant l’ensemble du pipeline.
- **Graph Database** : chargement des données finales dans Neo4j.


## Structure du projet

```bash
MEDAILLON-KG-PIPELINE/
│
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                         # API Fast API
│
├── dags/
│   └── ingest_kg.py                    # Pipeline Airflow
│   └── load_neo4j_from_gold.py         # Import Neo4j
│
├── data/
│   ├── raw/                            # Données générées
│   ├── bronze/                         # Parquet brut
│   ├── silver/                         # Parquet partitionné
│   └── gold/                           # CSV pour Neo4j
│
├── scripts/
│   ├── generate_sample_data.py         # Génération de données
│   ├── to_parquet.py                   # Conversion CSV→Parquet
│   ├── partition_edges.py              # Partitionnement
│   ├── export_gold_csv.py              # Export CSV pour Neo4j
│   └── load_neo4j_from_gold.py         # Chargement Neo4j
│
├── quality/
│   └── gx_checkpoint.py                # Validation Great Expectations
│
├── schema/
│   ├── neo4j-schema.cypher             # Contraintes Neo4j
│   └── ontology.ttl                    # Ontologie RDF (optionnel)
│
├── lineage/
│   └── openlineage.yml                 # Config OpenLineage
│
├── grafana/
│   ├── prometheus.yml                  # Config Prometheus
│   └── dashboards/                     # Dashboards Grafana
│
├── .env                                # Variables d'environnement
├── .gitignore
├── docker-compose.yaml                 # Services (Neo4j, Airflow, etc.)
├── LICENSE
├── Makefile                            # Commandes pratiques 
└── README.md
```

## Scripts Python

### 1. `generate_sample_data.py`
- Génère des données synthétiques :
  - `nodes.csv` : **id, label, name**
  - `edges.csv` : **src, dst, type**
- Par défaut : **1 million de nœuds** et **5 millions d’arêtes**.

Exécution :

```bash
# Avec paramètres explicites
python scripts/generate_sample_data.py --out data/raw --nodes 1000000 --edges 5000000

# Ou simplement (valeurs par defaut)
python scripts/generate_sample_data.py

# Ou via le Makefile
make seed
```

### 2. `to_parquet.py`
- Convertit les CSV bruts en **Parquet**.
- Entrées : `data/raw/nodes.csv`, `data/raw/edges.csv`
- Sorties : `data/bronze/nodes.parquet`, `data/bronze/edges.parquet`

Exécution :

```bash
# Avec paramètres explicites
python scripts/to_parquet.py --in data/raw --out data/bronze

# Ou simplement (valeurs par defaut)
python scripts/to_parquet.py

# Ou via le Makefile
make bronze
```

### 3. `gx_checkpoint.py`
- Vérifie la qualité des données **Bronze** avec **Great Expectations (GX)** :
- unicité de id dans nodes
- src et dst non nuls dans edges

```bash
# Vérification manuelle
python quality/gx_checkpoint.py --nodes data/bronze/nodes.parquet --edges data/bronze/edges.parquet
```
(Pas dans le Makefile car utilisé diretement dans le DAG Airflow)

### 4. `partition_edges.py`
- Partitionne les arêtes en ***N shards*** (par défaut 8).
- Copie les nœuds sans modification.
- Sortie :
  - `data/silver/nodes.parquet`
  - `data/silver/shard=0/edges.parquet`, …, `shard=7/edges.parquet`

Exécution :

```bash
# Avec valeurs par défaut
python scripts/partition_edges.py

# Ou via le Makefile
make silver
```

### 5. `export_gold_csv.py`
- Exporte les fichiers Silver en ***CSV Gold*** au format attendu par Neo4j :
  - `nodes.csv` : **id:ID, name, :LABEL**
  - `edges.csv` : **:START_ID, :END_ID**

Exécution :

```bash
# Avec valeurs par défaut
python scripts/export_gold_csv.py

# Ou via le Makefile
make gold
```

### 6. `load_neo4j_from_gold.py`
- Charge les CSV Gold dans Neo4j via **Cypher** (en batch) :
  - Création contrainte d’unicité sur **:Entity(id)**
  - MERGE des nœuds et arêtes
  - Évite les **doublons** si relancé

Exécution :

```bash
# Avec paramètres explicites
python scripts/load_neo4j_from_gold.py --uri bolt://localhost:7687 --nodes data/gold/nodes.csv --edges data/gold/edges.csv --batch 20000

# Ou simplement (valeurs par défaut)
python scripts/load_neo4j_from_gold.py
```
(Executé automatiquement dans le DAG load_neo4j_from_gold)


## Orchestration avec Apache Airflow

Deux DAGS sont définis :

### DAG 1 : `ingest_kg.py`
- Tâches :
  1. bronze_to_parquet → conversion CSV → Parquet
  2. gx_checkpoint → contrôle qualité Bronze
  3. partition_edges → création Silver
  4. export_gold_csv → création Gold
  5. trigger_load_neo4j_from_gold → déclenche le DAG 2 (chargement dans Neo4j)
  
  (J’avais testé d’utiliser un ExternalTaskSensor pour attendre la fin du DAG précédent, mais c’était plus complexe à mettre en place et moins clair. J’ai préféré garder le TriggerDagRunOperator, qui est plus simple et fonctionne bien dans ce cas.)

### DAG 2 : `load_neo4j_from_gold.py`
- Tâche unique qui charge les CSV Gold dans Neo4j:
  - load_neo4j → charge les fichiers Gold dans Neo4j


## Docker & Airflow

### Démararrer les services

```bash
make up
```

### Lancer Airflow (webserver + scheduler)

```bash
make airflow
```

### Lancer Neo4j

```bash
make neo4j
```
Accéder à l’UI Neo4j : http://localhost:7474

### Utilisation étape par étape

  1- Générer les données brutes :
  ```bash
  make seed
  ```

  2- Lancer le DAG d'ingestion :
  ```bash
  airflow dags trigger ingest_kg
  ```

  3- Lancer le DAG de chargement Neo4j (déclenché automatiquement) :
  ```bash
  airflow dags trigger load_neo4j_from_gold
  ```

### Vérification dans Neo4j

Exemple de requêtes :
  - Compter les nœuds :
  ```bash
  MATCH (n:Entity) RETURN count(n);
  ```

  - Compter les arretes :
  ```bash
  MATCH ()-[r:REL]->() RETURN count(r);
  ```

  ### Nettoyer Neo4j

  Si besoin de vider la base :
  ```bash
  MATCH (n) DETACH DELETE n;
  ```

  Ou coté Docker en supprimant les volumes :
  ```bash
  docker compose down -v
  ```

## Makefile disponible

Raccourcis utiles :
 - make up → lance tous les services
 - make down → arête tous les services
 - make airflow → démarre Airflow
 - make neo4j → démarre Neo4j
 - make seed → génère les données brutes
 - make bronze → CSV → Parquet
 - make silver → partitionnement
 - make gold → export CSV Gold

## Conclusion

Ce projet montre la mise en place complète d’un pipeline Médaillon orchestré par Airflow et connecté à Neo4j.
Chaque étape (Bronze, Silver, Gold) est claire, contrôlée et reproductible.

Le pipeline est extensible : il pourrait facilement intégrer d’autres contrôles de qualité, d’autres types de données, et une API pour exposer les résultats.
Dans une étape ultérieure, j’aimerais explorer la partie **API** et l’**observabilité**, afin de rendre ce pipeline encore plus complet et proche d’un cas industriel.