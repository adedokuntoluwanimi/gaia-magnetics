from typing import List, Dict


def merge_measured_and_predicted(
    train_rows: List[Dict],
    predicted_rows: List[Dict],
) -> List[Dict]:
    """
    Merges measured (train) rows with predicted rows.

    Rules:
    - Measured rows are preserved exactly
    - Predicted rows are added as-is
    - No snapping or replacement
    - A 'source' column differentiates rows
    - Final output is sorted by distance_along
    """

    merged = []

    # ----------------------------
    # Add measured rows
    # ----------------------------
    for row in train_rows:
        r = dict(row)
        r["source"] = "measured"
        merged.append(r)

    # ----------------------------
    # Add predicted rows
    # ----------------------------
    for row in predicted_rows:
        r = dict(row)
        r["source"] = "predicted"
        merged.append(r)

    # ----------------------------
    # Sort by distance along traverse
    # ----------------------------
    merged.sort(key=lambda r: float(r["distance_along"]))

    return merged
