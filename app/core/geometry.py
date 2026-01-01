# app/core/geometry.py

from math import hypot
from typing import List, Dict


def compute_distance_along_traverse(
    rows: List[Dict],
    x_col: str,
    y_col: str,
) -> List[Dict]:
    """
    Computes cumulative distance along traverse.
    Adds `d_along` to every row.
    """
    d = 0.0
    prev = None

    for r in rows:
        x = float(r[x_col])
        y = float(r[y_col])

        if prev is not None:
            d += hypot(x - prev[0], y - prev[1])

        r["d_along"] = d
        prev = (x, y)

    return rows


def generate_sparse_geometry(
    rows: List[Dict],
    *,
    x_col: str,
    y_col: str,
    value_col: str,
    spacing: float,
) -> List[Dict]:
    """
    Inserts uniform-spacing geometry rows between measured stations.

    Rules:
    - Original rows are measured
    - Inserted rows are unmeasured
    - Boundary stations remain measured
    """

    # Enforce ordering
    rows = sorted(rows, key=lambda r: r["d_along"])

    # Mark originals
    for r in rows:
        r["is_measured"] = True

    out: List[Dict] = []

    for i in range(len(rows) - 1):
        a = rows[i]
        b = rows[i + 1]

        out.append(a)

        da = a["d_along"]
        db = b["d_along"]
        gap = db - da

        if gap <= spacing:
            continue

        steps = int(gap // spacing)

        for k in range(1, steps + 1):
            d_new = da + k * spacing
            if d_new >= db:
                break

            t = (d_new - da) / gap

            out.append(
                {
                    x_col: float(a[x_col]) + t * (float(b[x_col]) - float(a[x_col])),
                    y_col: float(a[y_col]) + t * (float(b[y_col]) - float(a[y_col])),
                    "d_along": d_new,
                    value_col: "",
                    "is_measured": False,
                }
            )

    out.append(rows[-1])
    return out
