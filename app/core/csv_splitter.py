import csv
from typing import List, Dict, Tuple

from app.core.geometry import (
    compute_d_along,
    generate_sparse_geometry,
)


def read_csv(
    file_path: str,
    x_col: str,
    y_col: str,
    tmi_col: str,
) -> List[Dict]:
    """
    Read user CSV for a single traverse.

    Output rows contain:
    - x
    - y
    - tmi (None if missing)
    """

    rows = []

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for r in reader:
            x = float(r[x_col])
            y = float(r[y_col])

            tmi_raw = r.get(tmi_col)
            if tmi_raw is None or tmi_raw == "":
                tmi = None
            else:
                tmi = float(tmi_raw)

            rows.append({
                "x": x,
                "y": y,
                "tmi": tmi,
            })

    if not rows:
        raise ValueError("Uploaded CSV contains no rows")

    return rows


def split_explicit_geometry(
    rows: List[Dict],
) -> Tuple[List[Dict], List[Dict]]:
    """
    Explicit geometry:
    - Geometry is fully provided
    - Some rows have tmi, others don't
    """

    rows = compute_d_along(rows)

    train = []
    predict = []

    for r in rows:
        if r["tmi"] is not None:
            train.append({
                "x": r["x"],
                "y": r["y"],
                "d_along": r["d_along"],
                "tmi": r["tmi"],
                "is_measured": 1,
            })
        else:
            predict.append({
                "x": r["x"],
                "y": r["y"],
                "d_along": r["d_along"],
                "is_measured": 0,
            })

    return train, predict


def split_sparse_geometry(
    rows: List[Dict],
    spacing: float,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Sparse geometry:
    - Uploaded rows are measured stations only
    - Backend generates full geometry
    """

    full_rows = generate_sparse_geometry(
        rows=rows,
        station_spacing=spacing,
    )

    train = []
    predict = []

    for r in full_rows:
        if r["is_measured"] == 1:
            train.append({
                "x": r["x"],
                "y": r["y"],
                "d_along": r["d_along"],
                "tmi": r["tmi"],
                "is_measured": 1,
            })
        else:
            predict.append({
                "x": r["x"],
                "y": r["y"],
                "d_along": r["d_along"],
                "is_measured": 0,
            })

    return train, predict
