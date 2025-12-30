# app/core/merge.py

from typing import List
from app.core.geometry import GeometryRow


def merge_predictions(
    geometry: List[GeometryRow],
    predictions: List[float],
) -> List[GeometryRow]:
    """
    Merge predictions into geometry rows.

    Assumptions (LOCKED):
    - predictions correspond ONLY to rows where is_measured == 0
    - order is preserved
    - len(predictions) == number of unmeasured rows
    """

    output: List[GeometryRow] = []
    pred_idx = 0

    for row in geometry:
        if row.is_measured == 1:
            output.append(row)
        else:
            if pred_idx >= len(predictions):
                raise ValueError("Prediction count mismatch")

            output.append(
                GeometryRow(
                    x=row.x,
                    y=row.y,
                    d_along=row.d_along,
                    tmi=predictions[pred_idx],
                    is_measured=0,
                )
            )
            pred_idx += 1

    if pred_idx != len(predictions):
        raise ValueError("Unused predictions remain")

    return output
