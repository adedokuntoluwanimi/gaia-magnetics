import csv
from typing import List, Dict


def read_predictions_csv(path: str) -> List[float]:
    values = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            values.append(float(line))
    return values


def merge_measured_and_predicted(
    original_rows: List[Dict],
    predicted_values: List[float],
) -> List[Dict]:
    """
    Merge rule (LOCKED):

    - original_rows already contains:
        x, y, d_along, tmi (or None), is_measured
    - predicted_values are ordered
    - Assign predictions sequentially to rows where is_measured == 0
    - Never enforce strict count equality
    """

    merged = []
    pred_idx = 0
    total_preds = len(predicted_values)

    for row in original_rows:
        if row["is_measured"] == 1:
            merged.append(row)
        else:
            if pred_idx < total_preds:
                row["tmi"] = predicted_values[pred_idx]
                pred_idx += 1
            else:
                # No prediction available, leave as None
                row["tmi"] = None
            merged.append(row)

    return merged


def write_final_csv(rows: List[Dict], path: str) -> None:
    if not rows:
        return

    fieldnames = ["x", "y", "d_along", "tmi", "is_measured"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
