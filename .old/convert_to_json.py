import re
import json
import csv

#  Regex robusta → Cluster[count=87, x=498.26, y=129.25]
CLUSTER_RGX = re.compile(r"\{'x': ([\d.]+); 'y': ([\d.]+); 'count': (\d+)\}")


def csv_cluster_to_jsonl(input_file: str, output_file: str) -> int:
    records = []
    with open(input_file, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
                    if not row or len(row) < 5:
                        continue
                    seq_id, print_id, tile_id, saturated = row[:4]
                    centroids_raw = ",".join(row[4:])  # Unisce tutte le colonne rimanenti
                    centroids = []
                    centroids_raw = centroids_raw.strip()
                    if centroids_raw.startswith("["):
                        centroids_raw = centroids_raw[1:]
                    if centroids_raw.endswith("]"):
                        centroids_raw = centroids_raw[:-1]
                    for match in CLUSTER_RGX.finditer(centroids_raw):
                        x, y, count = match.groups()
                        centroids.append({
                            "x": float(x),
                            "y": float(y),
                            "count": int(count)
                        })
                    rec = {
                        "batch_id": int(seq_id),
                        "print_id": print_id,
                        "tile_id": int(tile_id),
                        "saturated": int(saturated),
                        "centroids": centroids
                    }
                    records.append(rec)
    with open(output_file, "w", encoding="utf-8") as out:
        for rec in records:
            json.dump(rec, out)
            out.write("\n")

    return len(records)

