# app/core/merge.py

import csv
from typing import List, Dict


def merge_predictions(
    geometry_rows: List[Dict],
    predictions_rows: List[Dict],
    value_col: str,
    pred_col: str = "predicted_value",
) -> List[Dict]:
    """
    Merge predicted values back into geometry rows.
    Geometry rows already contain is_measured and distance_along.
    """

    pred_map = {
        float(r["distance_along"]): r[pred_col]
        for r in predictions_rows
    }

    merged = []

    for r in geometry_rows:
        row = dict(r)
        d = float(row["distance_along"])

        if not row.get("is_measured", False):
            if d not in pred_map:
                raise ValueError(f"Missing prediction for distance {d}")
            row[value_col] = pred_map[d]

        merged.append(row)

    return merged


def write_csv(path: str, rows: List[Dict]):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
