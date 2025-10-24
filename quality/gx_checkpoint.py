"""
Valide la couche Bronze avec Great Expectations.

Entrées (Bronze) :
  - data/bronze/nodes.parquet   (doit avoir des id uniques)
  - data/bronze/edges.parquet   (src et dst non nul)

Règles :
  - unicité de 'id' dans nodes
  - 'src' et 'dst' non nuls dans edges
"""

from pathlib import Path
import sys
import pandas as pd
import great_expectations as ge
import os

# Chemins des fichiers Bronze
DATA_DIR = Path(os.getenv("KG_PIPELINE_DATA_DIR", "data"))
NODES_PARQUET = DATA_DIR / "bronze" / "nodes.parquet"
EDGES_PARQUET = DATA_DIR / "bronze" / "edges.parquet"


def check_nodes_unique_id(path: Path) -> None:
    """Vérifie l'unicité de la colonne id dans nodes."""
    if not path.exists():
        print(f"[GX][ERREUR] Fichier introuvable : {path}")
        sys.exit(1)

    df = pd.read_parquet(path)
    gdf = ge.from_pandas(df)
    res = gdf.expect_column_values_to_be_unique("id")
    if not res["success"]:
        raise SystemExit("[GX] ÉCHEC : doublons détectés dans nodes.id")
    print("[GX] nodes : id unique OK")


def check_edges_no_null_src_dst(path: Path) -> None:
    """Vérifie l'absence de valeurs nulles dans src et dst."""
    if not path.exists():
        print(f"[GX][ERREUR] Fichier introuvable : {path}")
        sys.exit(1)

    df = pd.read_parquet(path)
    gdf = ge.from_pandas(df)
    gdf.expect_column_values_to_not_be_null("src")
    gdf.expect_column_values_to_not_be_null("dst")
    res = gdf.validate()
    if not res["success"]:
        raise SystemExit("[GX] ÉCHEC : valeurs nulles dans edges.src/dst")
    print("[GX] edges : src/dst non nuls OK")


def main():
    check_nodes_unique_id(NODES_PARQUET)
    check_edges_no_null_src_dst(EDGES_PARQUET)
    print("[GX] checkpoint OK !")


if __name__ == "__main__":
    main()
