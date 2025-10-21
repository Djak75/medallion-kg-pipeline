"""
Export Gold : Parquet (Silver) -> CSV pour import Neo4j.

- Entrées :
  data/silver/nodes.parquet
  data/silver/shard=0..N-1/edges.parquet

- Sorties :
  data/gold/nodes.csv     (colonnes: id:ID,name,label)
  data/gold/edges.csv     (colonnes: :START_ID,:END_ID)

Usage :
  python scripts/export_gold_csv.py --silver data/silver --gold data/gold --partitions 8
"""

import argparse
from pathlib import Path
import pandas as pd

def parse_args():
    p = argparse.ArgumentParser(description="Export Gold CSV pour import Neo4j.")
    p.add_argument("--silver", type=str, default="data/silver", help="Dossier Silver (par défaut: data/silver)")
    p.add_argument("--gold",   type=str, default="data/gold",   help="Dossier Gold (par défaut: data/gold)")
    p.add_argument("--partitions", type=int, default=8, help="Nombre de shards (par défaut: 8)")
    return p.parse_args()

def main():
    args = parse_args()
    silver = Path(args.silver)
    gold   = Path(args.gold)
    gold.mkdir(parents=True, exist_ok=True)

    # 1) NODES: renommer colonnes pour Neo4j bulk
    #    demandé: id:ID, name, label
    df_nodes = pd.read_parquet(silver / "nodes.parquet")
    # On réordonne / renomme explicitement
    df_nodes = df_nodes[["id", "name", "label"]]
    df_nodes.columns = ["id:ID", "name", ":LABEL"]
    nodes_csv = gold / "nodes.csv"
    df_nodes.to_csv(nodes_csv, index=False)
    print(f"[gold] nodes -> {nodes_csv} ({len(df_nodes):,} lignes)")

    # 2) EDGES: concaténer les shards en un seul CSV
    #    demandé: :START_ID, :END_ID   (le type 'REL' sera fourni à l'import)
    edges_csv = gold / "edges.csv"
    header_written = False
    for s in range(args.partitions):
        shard_path = silver / f"shard={s}" / "edges.parquet"
        df = pd.read_parquet(shard_path)[["src", "dst"]]
        df.columns = [":START_ID", ":END_ID"]
        df.to_csv(edges_csv, mode="a", index=False, header=not header_written)
        header_written = True
        print(f"[gold] edges append from shard={s} ({len(df):,} lignes)")

    print("[gold] done")

if __name__ == "__main__":
    main()
