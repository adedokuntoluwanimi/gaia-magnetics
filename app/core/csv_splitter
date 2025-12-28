import csv
from typing import List, Dict, Tuple
from app.core.geometry import compute_d_along, generate_station_positions


def read_csv(
    file_path: str,
    x_col: str,
    y_col: str,
    tmi_col: str,
) -> List[Dict]:
    rows = []

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                "x": float(r[x_col]),
                "y": float(r[y_col]),
                "tmi": (
                    float(r[tmi_col])
                    if r[tmi_col] not in ("", None)
                    else None
                ),
            })

    return rows


def split_explicit_geometry(rows: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    train = []
    predict = []

    for r in rows:
        if r["tmi"] is None:
            predict.append(r)
        else:
            train.append(r)

    return train, predict


def split_sparse_geometry(
    rows: List[Dict],
    spacing: float,
    tolerance: float = 1e-6,
) -> Tuple[List[Dict], List[Dict]]:
    points = [(r["x"], r["y"]) for r in rows]
    d_along = compute_d_along(points)

    for r, d in zip(rows, d_along):
        r["d_along"] = d

    start_d = d_along[0]
    end_d = d_along[-1]

    generated_positions = generate_station_positions(
        start_d=start_d,
        end_d=end_d,
        spacing=spacing,
    )

    train = []
    predict = []

    for gp in generated_positions:
        matched = False

        for r in rows:
            if abs(r["d_along"] - gp) <= tolerance:
                train.append(r)
                matched = True
                break

        if not matched:
            predict.append({
                "d_along": gp,
            })

    return train, predict
