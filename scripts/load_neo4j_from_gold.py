"""
Charge les CSV Gold dans Neo4j (sans bulk import).

- Nodes : data/gold/nodes.csv  (colonnes: id:ID, name, :LABEL)
- Edges : data/gold/edges.csv  (colonnes: :START_ID, :END_ID)

"""

import argparse
from pathlib import Path
import pandas as pd
from neo4j import GraphDatabase

def parse_args():
    p = argparse.ArgumentParser(description="Load Gold CSV into Neo4j (Cypher)")
    p.add_argument("--uri", default="bolt://localhost:7687", help="Bolt URI de Neo4j")
    p.add_argument("--user", default="", help="Utilisateur Neo4j (vide si auth désactivée)")
    p.add_argument("--password", default="", help="Mot de passe Neo4j (vide si auth désactivée)")
    p.add_argument("--nodes", default="data/gold/nodes.csv", help="Chemin CSV nodes Gold")
    p.add_argument("--edges", default="data/gold/edges.csv", help="Chemin CSV edges Gold")
    p.add_argument("--batch", type=int, default=10000, help="Taille de lot pour l'UNWIND")
    return p.parse_args()

def ensure_constraints(driver):
    cypher = """
    CREATE CONSTRAINT entity_id IF NOT EXISTS
    FOR (n:Entity) REQUIRE n.id IS UNIQUE
    """
    with driver.session() as session:
        session.run(cypher)

def load_nodes(driver, nodes_csv: Path, batch: int):
    # Lecture par morceaux pour éviter la charge mémoire
    usecols = ["id:ID", "name", ":LABEL"]
    for chunk in pd.read_csv(nodes_csv, usecols=usecols, chunksize=batch):
        # On normalise les noms de colonnes
        chunk = chunk.rename(columns={"id:ID": "id", ":LABEL": "label"})
        rows = chunk.to_dict(orient="records")
        # Cypher : on crée :Entity, fixe name, puis on ajoute le label spécifique avec FOREACH
        cypher = """
        UNWIND $rows AS row
        MERGE (n:Entity {id: row.id})
        SET n.name = row.name
        FOREACH (_ IN CASE WHEN row.label = 'Person' THEN [1] ELSE [] END | SET n:Person)
        FOREACH (_ IN CASE WHEN row.label = 'Org'    THEN [1] ELSE [] END | SET n:Org)
        FOREACH (_ IN CASE WHEN row.label = 'Paper'  THEN [1] ELSE [] END | SET n:Paper)
        """
        with driver.session() as session:
            session.execute_write(lambda tx: tx.run(cypher, rows=rows))
    print("[neo4j] nodes chargés OK")

def load_edges(driver, edges_csv: Path, batch: int):
    usecols = [":START_ID", ":END_ID"]
    for chunk in pd.read_csv(edges_csv, usecols=usecols, chunksize=batch):
        chunk = chunk.rename(columns={":START_ID": "src", ":END_ID": "dst"})
        rows = chunk.to_dict(orient="records")
        cypher = """
        UNWIND $rows AS row
        MATCH (s:Entity {id: row.src})
        MATCH (d:Entity {id: row.dst})
        MERGE (s)-[:REL]->(d)
        """
        with driver.session() as session:
            session.execute_write(lambda tx: tx.run(cypher, rows=rows))
    print("[neo4j] edges chargés ✔")

def main():
    args = parse_args()
    nodes_csv = Path(args.nodes)
    edges_csv = Path(args.edges)

    auth = None
    if args.user or args.password:
        from neo4j import basic_auth
        auth = basic_auth(args.user, args.password)

    driver = GraphDatabase.driver(args.uri, auth=auth)

    ensure_constraints(driver)
    load_nodes(driver, nodes_csv, args.batch)
    load_edges(driver, edges_csv, args.batch)

    # Petit récap
    with driver.session() as session:
        cnt_nodes = session.run("MATCH (n:Entity) RETURN count(n) AS c").single()["c"]
        cnt_edges = session.run("MATCH ()-[r:REL]->() RETURN count(r) AS c").single()["c"]
    print(f"[neo4j] résumé : {cnt_nodes} nœuds, {cnt_edges} relations")

    driver.close()

if __name__ == "__main__":
    main()