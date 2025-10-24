"""
On exporte les données Silver (Parquet) en CSV Gold pour Neo4j.

Entrées (Silver) :
  - data/silver/nodes.parquet
  - data/silver/shard=0..N-1/edges.parquet

Sorties (Gold) :
  - data/gold/nodes.csv   (colonnes : id:ID, name, :LABEL)
  - data/gold/edges.csv   (colonnes : :START_ID, :END_ID)
"""

from pathlib import Path
import pandas as pd
import os

# Paramètres
DATA_DIR = Path(os.getenv("KG_PIPELINE_DATA_DIR", "data"))
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
N_PARTITIONS = 8


def main():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # Nettoyage : on supprime les anciens fichiers pour éviter que ça s’accumule
    nodes_out = GOLD_DIR / "nodes.csv"
    edges_out = GOLD_DIR / "edges.csv"
    if nodes_out.exists():
        nodes_out.unlink()
    if edges_out.exists():
        edges_out.unlink()

    # Export des nodes
    nodes_in = SILVER_DIR / "nodes.parquet"
    nodes = pd.read_parquet(nodes_in)[["id", "name", "label"]]
    nodes.columns = ["id:ID", "name", ":LABEL"]
    nodes.to_csv(nodes_out, index=False)
    print(f"[gold] nodes : {nodes_in} -> {nodes_out} ({len(nodes):,} lignes)")

    # Export des edges (concaténation des shards)
    total_edges = 0
    header_written = False

    for s in range(N_PARTITIONS):
        shard_in = SILVER_DIR / f"shard={s}" / "edges.parquet"
        edges = pd.read_parquet(shard_in)[["src", "dst"]]
        edges.columns = [":START_ID", ":END_ID"]
        edges.to_csv(edges_out, mode="a", index=False, header=not header_written)
        header_written = True
        total_edges += len(edges)
        print(f"[gold] shard {s} -> {edges_out} ({len(edges):,} lignes)")

    print(f"[gold] total edges écrits : {total_edges:,}")
    print("[gold] export terminé")


if __name__ == "__main__":
    main()
