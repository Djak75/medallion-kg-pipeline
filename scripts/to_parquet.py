"""
Convertit les CSV bruts (Bronze) en Parquet.

Entrée (par défaut) : data/raw/nodes.csv, data/raw/edges.csv
Sortie (par défaut) : data/bronze/nodes.parquet, data/bronze/edges.parquet

Usage :
  python scripts/to_parquet.py --in data/raw --out data/bronze
"""

import argparse
from pathlib import Path
import pandas as pd

def parse_args():
    p = argparse.ArgumentParser(description="CSV -> Parquet (Bronze)")
    p.add_argument("--in", dest="ind", type=str, default="data/raw", help="Dossier d'entrée CSV (par défaut: data/raw)")
    p.add_argument("--out", dest="outd", type=str, default="data/bronze", help="Dossier de sortie Parquet (par défaut: data/bronze)")
    return p.parse_args()

def main():
    args = parse_args()
    in_dir = Path(args.ind)
    out_dir = Path(args.outd)
    out_dir.mkdir(parents=True, exist_ok=True)

    nodes_csv = in_dir / "nodes.csv"
    edges_csv = in_dir / "edges.csv"
    nodes_parquet = out_dir / "nodes.parquet"
    edges_parquet = out_dir / "edges.parquet"

    # 1) nodes.csv -> nodes.parquet
    # Schéma : id(int64), label(string), name(string)
    df_nodes = pd.read_csv(
        nodes_csv,
        dtype={"id": "int64", "label": "string", "name": "string"}
    )
    df_nodes.to_parquet(nodes_parquet, index=False)  # compression par défaut (snappy via pyarrow)
    print(f"[bronze] {nodes_csv} -> {nodes_parquet} ({len(df_nodes):,} lignes)")

    # 2) edges.csv -> edges.parquet
    # Schéma : src(int64), dst(int64), type(string)
    df_edges = pd.read_csv(
        edges_csv,
        dtype={"src": "int64", "dst": "int64", "type": "string"}
    )
    df_edges.to_parquet(edges_parquet, index=False)
    print(f"[bronze] {edges_csv} -> {edges_parquet} ({len(df_edges):,} lignes)")

    print("[bronze] done")

if __name__ == "__main__":
    main()
