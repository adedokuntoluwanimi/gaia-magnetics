from typing import List, Dict, Tuple


def split_train_predict(
    rows: List[Dict],
    value_col: str,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Splits rows into train and predict datasets.

    Rules:
    - Train rows: rows that have a magnetic value
    - Predict rows: rows without a magnetic value
    - Geometry and distance_along must already exist
    - No geometry generation here
    - No reordering here

    Returns:
    - (train_rows, predict_rows)
    """

    train_rows = []
    predict_rows = []

    for row in rows:
        value = row.get(value_col)

        # Normalize empty strings
        if value == "":
            value = None

        if value is None:
            predict_row = dict(row)
            predict_row.pop(value_col, None)
            predict_rows.append(predict_row)
        else:
            train_row = dict(row)
            train_rows.append(train_row)

    if not train_rows:
        raise ValueError("No training data found. At least one measured value is required.")

    return train_rows, predict_rows
