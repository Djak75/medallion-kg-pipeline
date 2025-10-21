"""
Convertit les CSV bruts (Bronze) en fichiers Parquet.

Entrée  : data/raw/nodes.csv, data/raw/edges.csv
Sortie  : data/bronze/nodes.parquet, data/bronze/edges.parquet

Usage :
  python scripts/to_parquet.py --in data/raw --out data/bronze
"""

import argparse
import pandas as pd
from pathlib import Path

# --- Arguments en ligne de commande ---
parser = argparse.ArgumentParser(description="Conversion CSV -> Parquet")
parser.add_argument("--in", dest="in_dir", default="data/raw", help="Dossier d'entrée contenant les CSV")
parser.add_argument("--out", dest="out_dir", default="data/bronze", help="Dossier de sortie pour les Parquet")
args = parser.parse_args()

# --- Création du dossier de sortie ---
in_dir = Path(args.in_dir)
out_dir = Path(args.out_dir)
out_dir.mkdir(parents=True, exist_ok=True)

# --- Conversion des nodes ---
nodes_csv = in_dir / "nodes.csv"
nodes_parquet = out_dir / "nodes.parquet"

nodes = pd.read_csv(nodes_csv)              # lecture CSV
nodes.to_parquet(nodes_parquet, index=False)  # écriture en Parquet
print(f"[bronze] {nodes_csv} -> {nodes_parquet} ({len(nodes)} lignes)")

# --- Conversion des edges ---
edges_csv = in_dir / "edges.csv"
edges_parquet = out_dir / "edges.parquet"

edges = pd.read_csv(edges_csv)
edges.to_parquet(edges_parquet, index=False)
print(f"[bronze] {edges_csv} -> {edges_parquet} ({len(edges)} lignes)")

print("[bronze] conversion terminée")
