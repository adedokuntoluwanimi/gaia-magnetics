from typing import List, Dict


def merge_measured_and_predicted(
    train_rows: List[Dict],
    predicted_rows: List[Dict],
    value_col: str = "magnetic_value",
) -> List[Dict]:
    """
    Merge measured and predicted rows into a final ordered dataset.

    Rules:
    - Measured rows are preserved exactly
    - Predicted rows are appended without overwriting
    - A 'source' field identifies row origin
    - Output is sorted strictly by distance_along
    """

    merged = []

    # ----------------------------
    # Measured rows
    # ----------------------------
    for row in train_rows:
        if "distance_along" not in row:
            raise ValueError("Measured row missing distance_along")

        if value_col not in row:
            raise ValueError("Measured row missing magnetic value")

        merged.append({
            "distance_along": float(row["distance_along"]),
            "magnetic_value": float(row[value_col]),
            "source": "measured",
        })

    # ----------------------------
    # Predicted rows
    # ----------------------------
    for row in predicted_rows:
        if "distance_along" not in row:
            raise ValueError("Predicted row missing distance_along")

        if value_col not in row:
            raise ValueError("Predicted row missing magnetic value")

        merged.append({
            "distance_along": float(row["distance_along"]),
            "magnetic_value": float(row[value_col]),
            "source": "predicted",
        })

    # ----------------------------
    # Sort
    # ----------------------------
    merged.sort(key=lambda r: r["distance_along"])

    return merged
