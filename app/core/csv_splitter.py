# app/core/csv_splitter.py

from typing import List, Dict, Tuple


def split_train_predict(
    rows: List[Dict],
    *,
    value_col: str,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Splits rows into train and predict sets.

    Rules:
    - train: is_measured == True AND value exists
    - predict: is_measured == False AND value missing
    """

    train: List[Dict] = []
    predict: List[Dict] = []

    for r in rows:
        is_measured = bool(r.get("is_measured", False))
        val = r.get(value_col, "")

        has_value = val not in ("", None)

        if is_measured and has_value:
            train.append(r)

        elif (not is_measured) and (not has_value):
            predict.append(r)

    return train, predict
