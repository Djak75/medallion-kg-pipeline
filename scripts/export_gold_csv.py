"""
Export Gold : Silver (Parquet) -> Gold (CSV)

Entrées :
  - data/silver/nodes.parquet
  - data/silver/shard=0..N-1/edges.parquet

Sorties :
  - data/gold/nodes.csv   (colonnes : id:ID, name, :LABEL)
  - data/gold/edges.csv   (colonnes : :START_ID, :END_ID)

Usage :
  python scripts/export_gold_csv.py --silver data/silver --gold data/gold --partitions 8
"""

import argparse
import pandas as pd
from pathlib import Path

# --- Arguments en ligne de commande ---
parser = argparse.ArgumentParser(description="Export Gold CSV depuis Silver (Parquet)")
parser.add_argument("--silver", default="data/silver", help="Dossier Silver (parquet)")
parser.add_argument("--gold", default="data/gold", help="Dossier Gold (csv)")
parser.add_argument("--partitions", type=int, default=8, help="Nombre de shards")
args = parser.parse_args()

silver = Path(args.silver)
gold = Path(args.gold)
gold.mkdir(parents=True, exist_ok=True)

# 1- Export des nodes
nodes_in = silver / "nodes.parquet"
nodes_out = gold / "nodes.csv"

nodes = pd.read_parquet(nodes_in)[["id", "name", "label"]]
nodes.columns = ["id:ID", "name", ":LABEL"]  # format attendu par Neo4j
nodes.to_csv(nodes_out, index=False)

print(f"[gold] nodes : {nodes_in} -> {nodes_out} ({len(nodes)} lignes)")

# 2- Export des edges (concaténation de tous les shards)
edges_out = gold / "edges.csv"
header_written = False
total_edges = 0

for s in range(args.partitions):
    shard_in = silver / f"shard={s}" / "edges.parquet"
    edges = pd.read_parquet(shard_in)[["src", "dst"]]
    edges.columns = [":START_ID", ":END_ID"]
    edges.to_csv(edges_out, mode="a", index=False, header=not header_written)
    header_written = True
    total_edges += len(edges)
    print(f"[gold] shard {s} -> edges.csv ({len(edges)} lignes)")

print(f"[gold] total edges écrits : {total_edges}")
print("[gold] export terminé")
