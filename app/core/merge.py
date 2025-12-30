from typing import List, Dict


def merge_measured_and_predicted(
    measured_rows: List[Dict],
    predicted_rows: List[Dict],
    predictions: List[float],
) -> List[Dict]:
    """
    measured_rows:
        rows that already have values

    predicted_rows:
        rows that need values (same order used for inference)

    predictions:
        model output, same length as predicted_rows
    """

    if len(predicted_rows) != len(predictions):
        raise RuntimeError(
            "Prediction count mismatch during merge"
        )

    merged: List[Dict] = []

    # 1. Keep measured rows exactly as they are
    for row in measured_rows:
        merged.append({
            **row,
            "source": "measured",
        })

    # 2. Attach predictions to predicted rows
    for row, value in zip(predicted_rows, predictions):
        merged.append({
            **row,
            "value": value,
            "source": "predicted",
        })

    return merged
