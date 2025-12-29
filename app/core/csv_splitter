import csv
from typing import List, Dict, Tuple

from app.core.geometry import (
    compute_d_along,
    generate_station_positions,
)


# ======================================================
# CSV READER
# ======================================================
def read_csv(
    file_path: str,
    x_col: str,
    y_col: str,
    tmi_col: str,
) -> List[Dict]:
    """
    Read CSV and return normalized rows.
    Rows with invalid or missing coordinates are skipped.
    TMI may be None (explicit geometry).
    """

    rows: List[Dict] = []

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for r in reader:
            # ----------------------------------
            # Validate coordinates
            # ----------------------------------
            x_raw = r.get(x_col)
            y_raw = r.get(y_col)

            if x_raw in ("", None) or y_raw in ("", None):
                continue

            try:
                x = float(x_raw)
                y = float(y_raw)
            except ValueError:
                continue

            # ----------------------------------
            # Handle TMI (optional)
            # ----------------------------------
            tmi_raw = r.get(tmi_col)
            tmi = None

            if tmi_raw not in ("", None):
                try:
                    tmi = float(tmi_raw)
                except ValueError:
                    tmi = None

            rows.append({
                "x": x,
                "y": y,
                "tmi": tmi,
            })

    if not rows:
        raise ValueError("No valid rows found in CSV")

    return rows


# ======================================================
# EXPLICIT GEOMETRY
# ======================================================
def split_explicit_geometry(
    rows: List[Dict],
) -> Tuple[List[Dict], List[Dict]]:
    """
    Explicit geometry:
    - Rows with TMI -> train
    - Rows without TMI -> predict
    """

    train: List[Dict] = []
    predict: List[Dict] = []

    points = [(r["x"], r["y"]) for r in rows]
    d_along = compute_d_along(points)

    for r, d in zip(rows, d_along):
        r_out = {
            "x": r["x"],
            "y": r["y"],
            "d_along": d,
        }

        if r["tmi"] is None:
            predict.append(r_out)
        else:
            r_out["tmi"] = r["tmi"]
            train.append(r_out)

    if not train:
        raise ValueError("Explicit geometry requires at least one measured point")

    return train, predict


# ======================================================
# SPARSE GEOMETRY
# ======================================================
def split_sparse_geometry(
    rows: List[Dict],
    spacing: float,
    tolerance: float = 1e-6,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Sparse geometry:
    - Input rows must ALL have TMI
    - Generate new stations using user-defined spacing
    - Match original points by d_along
    """

    if spacing <= 0:
        raise ValueError("station_spacing must be > 0")

    # ----------------------------------
    # Validate sparse input
    # ----------------------------------
    for r in rows:
        if r["tmi"] is None:
            raise ValueError("Sparse geometry CSV cannot contain empty TMI values")

    # ----------------------------------
    # Compute geometry
    # ----------------------------------
    points = [(r["x"], r["y"]) for r in rows]
    d_along = compute_d_along(points)

    measured_by_d = {}
    for r, d in zip(rows, d_along):
        measured_by_d[d] = {
            "x": r["x"],
            "y": r["y"],
            "tmi": r["tmi"],
            "d_along": d,
        }

    start_d = d_along[0]
    end_d = d_along[-1]

    generated_positions = generate_station_positions(
        start_d=start_d,
        end_d=end_d,
        spacing=spacing,
    )

    train: List[Dict] = []
    predict: List[Dict] = []

    # ----------------------------------
    # Split measured vs generated
    # ----------------------------------
    for gp in generated_positions:
        matched = False

        for md, row in measured_by_d.items():
            if abs(md - gp) <= tolerance:
                train.append(row)
                matched = True
                break

        if not matched:
            predict.append({
                "x": None,
                "y": None,
                "d_along": gp,
            })

    if not train:
        raise ValueError("No training points generated from sparse geometry")

    return train, predict
