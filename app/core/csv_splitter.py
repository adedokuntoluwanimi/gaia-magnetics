from typing import List, Dict, Tuple


def _has_value(row: Dict, value_col: str) -> bool:
    """
    Determines whether a row contains a usable magnetic value.
    """
    if value_col not in row:
        return False

    val = row[value_col]

    if val is None:
        return False

    if isinstance(val, str):
        return val.strip() != ""

    return True


def split_train_predict(
    rows: List[Dict],
    value_col: str,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Splits rows into training and prediction sets.

    Rules:
    - Train rows: value_col exists and is non-empty
    - Predict rows: value_col missing or empty
    - distance_along must already exist
    """

    train_rows = []
    predict_rows = []

    for row in rows:
        if "distance_along" not in row:
            raise ValueError("distance_along missing before split")

        if _has_value(row, value_col):
            r = dict(row)
            r[value_col] = float(r[value_col])
            train_rows.append(r)
        else:
            r = dict(row)
            r.pop(value_col, None)
            predict_rows.append(r)

    return train_rows, predict_rows
