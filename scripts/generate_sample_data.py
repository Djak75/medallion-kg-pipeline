"""
Génération de données synthétiques pour Knowledge Graph

- nodes.csv : id,label,name
- edges.csv : src,dst,type

Par défaut : 1_000_000 nodes, 5_000_000 edges.
Utilisation :
  python scripts/generate_sample_data.py --out data/raw --nodes 1000000 --edges 5000000
"""

import os
import csv
import argparse
import random
from pathlib import Path

LABELS = ["Person", "Org", "Paper"]
EDGE_TYPE = "REL"
RANDOM_SEED = 42  # pour rendre la génération reproductible

def ensure_dir(path: Path) -> None:
    """Crée le dossier de sortie s'il n'existe pas."""
    path.mkdir(parents=True, exist_ok=True)

def gen_nodes(out_dir: Path, n_nodes: int) -> Path:
    """Génère nodes.csv avec les colonnes id,label,name."""
    nodes_path = out_dir / "nodes.csv"
    with nodes_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "label", "name"])
        for i in range(n_nodes):
            label = random.choice(LABELS)
            writer.writerow([i, label, f"name_{i}"])
    return nodes_path

def gen_edges(out_dir: Path, n_nodes: int, n_edges: int) -> Path:
    """Génère edges.csv avec les colonnes src,dst,type."""
    edges_path = out_dir / "edges.csv"
    with edges_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["src", "dst", "type"])
        for _ in range(n_edges):
            src = random.randrange(n_nodes)
            dst = random.randrange(n_nodes)
            if dst == src:                    # évite une boucle sur soi-même
                dst = (dst + 1) % n_nodes
            writer.writerow([src, dst, EDGE_TYPE])
    return edges_path

def parse_args():
    """Parse les arguments CLI : dossier de sortie, nb de noeuds, nb d'arêtes."""
    p = argparse.ArgumentParser(description="Génère nodes.csv et edges.csv (données synthétiques KG).")
    p.add_argument("--out", type=str, default="data/raw", help="Dossier de sortie (par défaut: data/raw)")
    p.add_argument("--nodes", type=int, default=1_000_000, help="Nombre de nœuds (par défaut: 1_000_000)")
    p.add_argument("--edges", type=int, default=5_000_000, help="Nombre d’arêtes (par défaut: 5_000_000)")
    return p.parse_args()

def main():
    args = parse_args()
    out_dir = Path(args.out)
    ensure_dir(out_dir)

    random.seed(RANDOM_SEED)

    nodes_path = gen_nodes(out_dir, args.nodes)
    print(f"[seed] nodes -> {nodes_path}")

    edges_path = gen_edges(out_dir, args.nodes, args.edges)
    print(f"[seed] edges -> {edges_path}")

    print("[seed] done")

if __name__ == "__main__":
    main()