"""
Génération de données factices pour tester le pipeline (couche Raw).

Par défaut, on génère :
- 1 000 000 nodes
- 5 000 000 edges

Format des fichiers générés :
- nodes.csv : id,label,name
- edges.csv : src,dst,type
"""

import csv
import random
from pathlib import Path
import os

# Paramètres
DATA_DIR = Path(os.getenv("KG_PIPELINE_DATA_DIR", "data"))
OUT_DIR = DATA_DIR / "raw"
N_NODES = 1_000_000
N_EDGES = 5_000_000
LABELS = ["Person", "Org", "Paper"]
EDGE_TYPE = "REL"
RANDOM_SEED = 42  # reproductible


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    random.seed(RANDOM_SEED)

    # nodes.csv
    nodes_path = OUT_DIR / "nodes.csv"
    with nodes_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "label", "name"])
        for i in range(N_NODES):
            w.writerow([i, random.choice(LABELS), f"name_{i}"])
    print(f"[seed] {nodes_path} généré ({N_NODES:,} lignes)")

    # edges.csv
    edges_path = OUT_DIR / "edges.csv"
    with edges_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["src", "dst", "type"])
        for _ in range(N_EDGES):
            src = random.randrange(N_NODES)
            dst = random.randrange(N_NODES)
            if dst == src:  # Comme ça j'évite une boucle sur soi-même
                dst = (dst + 1) % N_NODES
            w.writerow([src, dst, EDGE_TYPE])
    print(f"[seed] {edges_path} généré ({N_EDGES:,} lignes)")

    print("[seed] terminé")


if __name__ == "__main__":
    main()