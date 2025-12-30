# app/core/split.py

from typing import List, Tuple
from app.core.geometry import GeometryRow


def split_train_predict(
    rows: List[GeometryRow],
) -> Tuple[List[GeometryRow], List[GeometryRow]]:
    """
    Split geometry rows into train and predict sets.

    Rule (locked):
    - Train = rows with is_measured == 1
    - Predict = rows with is_measured == 0
    """

    train: List[GeometryRow] = []
    predict: List[GeometryRow] = []

    for r in rows:
        if r.is_measured == 1:
            train.append(r)
        else:
            predict.append(r)

    if not train:
        raise ValueError("No measured rows available for training")

    return train, predict
