import csv
import os
import re
import numpy as np  # solo per stampare np.float64
import json

#  Regex robusta → Cluster[count=87, x=498.26, y=129.25]
CLUSTER_RGX = re.compile(
    r"Cluster\[count=(\d+),\s*x=([\d.]+),\s*y=([\d.]+)\]"
)


def csv_cluster_to_jsonl(input_file: str, output_file: str) -> int:

    def parse_line(raw: str):
        parts = raw.split(",", 4)
        if len(parts) < 5:
            return None
        seq_id, print_id, tile_id, saturated, centroids_raw = parts

        centroids_raw = centroids_raw.strip()
        if centroids_raw.startswith("["):
            centroids_raw = centroids_raw[1:]
        if centroids_raw.endswith("]"):
            centroids_raw = centroids_raw[:-1]

        # estrai cluster
        centroids = [
            {
                "x": f"np.float64({x})",
                "y": f"np.float64({y})",
                "count": int(cnt),
            }
            for cnt, x, y in CLUSTER_RGX.findall(centroids_raw)
        ]

        return {
            "batch_id": int(seq_id),
            "print_id": print_id,
            "tile_id": int(tile_id),
            "saturated": int(saturated),
            "centroids": centroids,
        }

    records = []
    with open(input_file, encoding="utf-8") as f:
        next(f, None)  # salta header
        for raw in f:
            raw = raw.rstrip("\n")
            if not raw:
                continue
            rec = parse_line(raw)
            if rec:
                records.append(rec)

    with open(output_file, "w", encoding="utf-8") as out:
        for rec in records:
            json.dump(rec, out)
            out.write("\n")

    return len(records)


