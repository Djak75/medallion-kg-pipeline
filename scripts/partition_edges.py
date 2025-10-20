"""
Partitionne les edges en N shards (Silver) et copie les nodes tels quels.

Entrée :
  - data/bronze/nodes.parquet
  - data/bronze/edges.parquet

Sortie :
  - data/silver/nodes.parquet
  - data/silver/shard=0/edges.parquet
    ...
  - data/silver/shard=N-1/edges.parquet

Usage :
  python scripts/partition_edges.py --in data/bronze --out data/silver --partitions 8
"""

import argparse
from pathlib import Path
import pandas as pd
import shutil

def parse_args():
    p = argparse.ArgumentParser(description="Partitionne edges en shards et copie nodes (Silver).")
    p.add_argument("--in",  dest="ind",  type=str, default="data/bronze", help="Dossier d'entrée Parquet (par défaut: data/bronze)")
    p.add_argument("--out", dest="outd", type=str, default="data/silver", help="Dossier de sortie (par défaut: data/silver)")
    p.add_argument("--partitions", type=int, default=8, help="Nombre de shards (par défaut: 8)")
    return p.parse_args()

def main():
    args = parse_args()
    in_dir  = Path(args.ind)
    out_dir = Path(args.outd)
    out_dir.mkdir(parents=True, exist_ok=True)

    nodes_in  = in_dir / "nodes.parquet"
    edges_in  = in_dir / "edges.parquet"
    nodes_out = out_dir / "nodes.parquet"

    # 1) Copier nodes.parquet comme ils sont
    shutil.copy2(nodes_in, nodes_out)
    print(f"[silver] nodes copied: {nodes_in} -> {nodes_out}")

    # 2) Lire edges et ajouter une colonne shard (modulo sur 'SRC' pour distribuer)
    df_edges = pd.read_parquet(edges_in)
    n = args.partitions
    df_edges["shard"] = (df_edges["src"] % n).astype("int64")

    # 3) Écrire un Parquet par shard
    for s in range(n):
        shard_dir = out_dir / f"shard={s}"
        shard_dir.mkdir(parents=True, exist_ok=True)
        shard_path = shard_dir / "edges.parquet"
        df_s = df_edges[df_edges["shard"] == s].drop(columns=["shard"])
        df_s.to_parquet(shard_path, index=False)
        print(f"[silver] shard {s}: {len(df_s):,} edges -> {shard_path}")

    print("[silver] done")

# Exécution principale
if __name__ == "__main__":
    main()
