"""
On partitionne les edges en plusieurs shards (Silver) et on copie les nodes

Entrées (Bronze) :
  data/bronze/nodes.parquet
  data/bronze/edges.parquet

Sorties (Silver) :
  data/silver/nodes.parquet
  data/silver/shard=0/edges.parquet
  ...
  data/silver/shard=7/edges.parquet
"""

from pathlib import Path
import shutil
import pandas as pd
import os

# Paramètres
DATA_DIR = Path(os.getenv("KG_PIPELINE_DATA_DIR", "data"))
IN_DIR = DATA_DIR / "bronze"
OUT_DIR = DATA_DIR / "silver"
N_PARTITIONS = 8   # nombre de shards


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # On copie les nodes comme ils sont
    nodes_in = IN_DIR / "nodes.parquet"
    nodes_out = OUT_DIR / "nodes.parquet"
    shutil.copy2(nodes_in, nodes_out)
    print(f"[silver] nodes copiés : {nodes_in} -> {nodes_out}")

    # On partitionne les edges par shard
    edges_in = IN_DIR / "edges.parquet"
    df_edges = pd.read_parquet(edges_in)
    df_edges["shard"] = df_edges["src"] % N_PARTITIONS

    total = 0
    # On écrit chaque shard dans son dossier
    for s in range(N_PARTITIONS):
        shard_dir = OUT_DIR / f"shard={s}"
        shard_dir.mkdir(parents=True, exist_ok=True)
        shard_path = shard_dir / "edges.parquet"

        part = df_edges[df_edges["shard"] == s].drop(columns=["shard"])
        part.to_parquet(shard_path, index=False)
        total += len(part)

        print(f"[silver] shard {s} -> {shard_path} ({len(part):,} lignes)")

    print(f"[silver] total edges répartis : {total:,}")
    print("[silver] terminé")


if __name__ == "__main__":
    main()
