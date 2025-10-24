"""
Charge les CSV Gold dans Neo4j (sans bulk, via Cypher en batch)

Entrées (Gold) :
  - data/gold/nodes.csv   colonnes : id:ID, name, :LABEL
  - data/gold/edges.csv   colonnes : :START_ID, :END_ID

Comportement :
  - crée une contrainte d’unicité sur :Entity(id) si absente
  - MERGE des nœuds (idempotent) + met à jour name + garde le label comme propriété
  - MERGE des relations :REL (idempotent)
  - lecture en batchs pour limiter la mémoire
"""

from pathlib import Path
import os
import pandas as pd
from neo4j import GraphDatabase

# Paramètres
DATA_DIR   = Path(os.getenv("KG_PIPELINE_DATA_DIR", "data"))
NODES_CSV  = DATA_DIR / "gold" / "nodes.csv"
EDGES_CSV  = DATA_DIR / "gold" / "edges.csv"
BATCH_SIZE = 20_000

# Par défaut on est en local sans auth
NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")


def create_constraint(session) -> None:
    """Crée la contrainte d’unicité sur :Entity(id) si elle n’existe pas."""
    session.run("""
        CREATE CONSTRAINT entity_id IF NOT EXISTS
        FOR (n:Entity) REQUIRE n.id IS UNIQUE
    """).consume()


def load_nodes(session) -> None:
    """
    Charge les nœuds par batch.
    - On renomme les colonnes du CSV (id:ID -> id, :LABEL -> label).
    - On stocke le label métier en propriété (n.label).
    """
    total = 0
    for chunk in pd.read_csv(NODES_CSV, chunksize=BATCH_SIZE, usecols=["id:ID", "name", ":LABEL"]):
        chunk = chunk.rename(columns={"id:ID": "id", ":LABEL": "label"})
        rows = chunk[["id", "name", "label"]].to_dict("records")

        # Cypher avec UNWIND + MERGE
        session.run(
            """
            UNWIND $rows AS row
            MERGE (n:Entity {id: row.id})
            SET n.name = row.name,
                n.label = row.label
            """,
            rows=rows,
        ).consume()

        total += len(rows)
        print(f"[neo4j] nodes batch OK (+{len(rows):,}) total={total:,}")

    print(f"[neo4j] nodes chargés : {total:,}")


def load_edges(session) -> None:
    """
    Charge les relations par batch.
    - On renomme :START_ID/:END_ID en src/dst.
    - MERGE évite les doublons si on relance le script.
    """
    total = 0
    for chunk in pd.read_csv(EDGES_CSV, chunksize=BATCH_SIZE, usecols=[":START_ID", ":END_ID"]):
        chunk = chunk.rename(columns={":START_ID": "src", ":END_ID": "dst"})
        rows = chunk[["src", "dst"]].to_dict("records")

        session.run(
            """
            UNWIND $rows AS row
            MATCH (s:Entity {id: row.src})
            MATCH (d:Entity {id: row.dst})
            MERGE (s)-[:REL]->(d)
            """,
            rows=rows,
        ).consume()

        total += len(rows)
        print(f"[neo4j] edges batch OK (+{len(rows):,}) total={total:,}")

    print(f"[neo4j] edges chargées : {total:,}")


def main():
    # Authentification si fournie
    auth = None
    if NEO4J_USER or NEO4J_PASSWORD:
        from neo4j import basic_auth
        auth = basic_auth(NEO4J_USER, NEO4J_PASSWORD)

    driver = GraphDatabase.driver(NEO4J_URI, auth=auth)

    with driver.session() as session:
        create_constraint(session)
        load_nodes(session)
        load_edges(session)

        # Résumé
        cnt_nodes = session.run("MATCH (n:Entity) RETURN count(n) AS c").single()["c"]
        cnt_edges = session.run("MATCH ()-[r:REL]->() RETURN count(r) AS c").single()["c"]
        print(f"[neo4j] résumé : {cnt_nodes:,} nœuds, {cnt_edges:,} relations")

    driver.close()


if __name__ == "__main__":
    main()