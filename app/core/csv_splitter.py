from typing import List, Dict, Tuple


def split_train_predict(
    rows: List[Dict],
    *,
    value_col: str,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Splits rows into train and predict sets.

    Assumptions (ENFORCED):
    - rows contain distance_along
    - rows contain is_measured as a boolean
    - value_col exists

    Rules:
    - Train rows:
        is_measured == True AND value exists
    - Predict rows:
        is_measured == False AND value is empty
    """

    train: List[Dict] = []
    predict: List[Dict] = []

    for r in rows:
        if "is_measured" not in r:
            raise RuntimeError("Row missing is_measured flag")

        if not isinstance(r["is_measured"], bool):
            raise RuntimeError(
                f"is_measured must be boolean, got {type(r['is_measured'])}"
            )

        val = r.get(value_col)

        has_value = val not in ("", None)

        if r["is_measured"] and has_value:
            train.append(r)

        elif (not r["is_measured"]) and (not has_value):
            predict.append(r)

    return train, predict
