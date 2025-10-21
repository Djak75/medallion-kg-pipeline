"""
On partitionne les edges en 8 shards (Silver) et copie les nodes tels quels.

Entrée  (Bronze) : data/bronze/nodes.parquet, data/bronze/edges.parquet
Sortie  (Silver) : data/silver/nodes.parquet, data/silver/shard=.../edges.parquet

Usage :
  python scripts/partition_edges.py --in data/bronze --out data/silver --partitions 8
"""

import argparse
from pathlib import Path
import shutil
import pandas as pd

# --- Arguments en ligne de commande ---
parser = argparse.ArgumentParser(description="Silver : partitionner les edges et copier les nodes")
parser.add_argument("--in",  dest="in_dir",  default="data/bronze", help="Dossier d'entrée (Bronze)")
parser.add_argument("--out", dest="out_dir", default="data/silver", help="Dossier de sortie (Silver)")
parser.add_argument("--partitions", type=int, default=8, help="Nombre de shards (par défaut : 8)")
args = parser.parse_args()

in_dir  = Path(args.in_dir)
out_dir = Path(args.out_dir)
out_dir.mkdir(parents=True, exist_ok=True)

# --- Chemins utiles ---
nodes_in  = in_dir / "nodes.parquet"
edges_in  = in_dir / "edges.parquet"
nodes_out = out_dir / "nodes.parquet"

# 1- On copie les nodes de Bronze -> Silver
shutil.copy2(nodes_in, nodes_out)
print(f"[silver] nodes copiés : {nodes_in} -> {nodes_out}")

# 2- On lit les edges et on calcule un numéro de shard
#    Règle simple : on distribue par (src % N) pour une répartition stable.
df_edges = pd.read_parquet(edges_in)
n = int(args.partitions)
df_edges["shard"] = (df_edges["src"] % n).astype("int64")

# 3- Écrire un fichier Parquet par shard
#    Structure : data/silver/shard=0/edges.parquet, ..., shard=N-1/edges.parquet
total = 0
for s in range(n):
    shard_dir = out_dir / f"shard={s}"
    shard_dir.mkdir(parents=True, exist_ok=True)
    shard_path = shard_dir / "edges.parquet"

    part = df_edges[df_edges["shard"] == s].drop(columns=["shard"])
    part.to_parquet(shard_path, index=False)
    total += len(part)

    print(f"[silver] shard {s} -> {shard_path} ({len(part)} lignes)")

print(f"[silver] total edges répartis : {total}")
print("[silver] terminé")
