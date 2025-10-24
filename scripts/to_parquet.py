"""
to_parquet.py — version simple
-------------------------------
Lit les CSV bruts et écrit deux Parquet pour la couche Bronze.

Entrées attendues :
  data/raw/nodes.csv
  data/raw/edges.csv

Sorties produites :
  data/bronze/nodes.parquet
  data/bronze/edges.parquet
"""

from pathlib import Path
import pandas as pd
import sys
import os

# Chemins par défaut
DATA_DIR = Path(os.getenv("KG_PIPELINE_DATA_DIR", "data"))
IN_DIR   = DATA_DIR / "raw"
OUT_DIR  = DATA_DIR / "bronze"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # nodes
    nodes_csv = IN_DIR / "nodes.csv"
    nodes_parquet = OUT_DIR / "nodes.parquet"
    if not nodes_csv.exists():
        print(f"[ERREUR] Fichier introuvable : {nodes_csv}")
        sys.exit(1)

    nodes = pd.read_csv(nodes_csv)
    nodes.to_parquet(nodes_parquet, index=False)
    print(f"[bronze] {nodes_csv} -> {nodes_parquet} ({len(nodes):,} lignes)")

    # edges
    edges_csv = IN_DIR / "edges.csv"
    edges_parquet = OUT_DIR / "edges.parquet"
    if not edges_csv.exists():
        print(f"[ERREUR] Fichier introuvable : {edges_csv}")
        sys.exit(1)

    edges = pd.read_csv(edges_csv)
    edges.to_parquet(edges_parquet, index=False)
    print(f"[bronze] {edges_csv} -> {edges_parquet} ({len(edges):,} lignes)")

    print("[bronze] terminé ")


if __name__ == "__main__":
    main()
