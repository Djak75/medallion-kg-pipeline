"""
GX pour valider Bronze :
- unicité de id dans nodes
- src et dst non nuls dans edges
"""
import argparse
import pandas as pd
import great_expectations as ge  

def check_nodes_unique_id(nodes_path: str) -> None:
    df = pd.read_parquet(nodes_path)
    gdf = ge.from_pandas(df)  
    res = gdf.expect_column_values_to_be_unique("id")
    if not res["success"]:
        raise SystemExit("GX failed: duplicate ids in nodes")

def check_edges_no_null_src_dst(edges_path: str) -> None:
    df = pd.read_parquet(edges_path)
    gdf = ge.from_pandas(df)
    gdf.expect_column_values_to_not_be_null("src")
    gdf.expect_column_values_to_not_be_null("dst")
    res = gdf.validate()
    if not res["success"]:
        raise SystemExit("GX failed: nulls in src/dst")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", default="data/bronze/nodes.parquet")
    ap.add_argument("--edges", default="data/bronze/edges.parquet")
    args = ap.parse_args()

    check_nodes_unique_id(args.nodes)
    print("[GX] nodes: id unique OK")

    check_edges_no_null_src_dst(args.edges)
    print("[GX] edges: src/dst non nuls OK")

    print("[GX] checkpoint OK")

if __name__ == "__main__":
    main()
