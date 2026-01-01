from math import hypot
from typing import List, Dict


# ==================================================
# Explicit geometry
# ==================================================
def compute_distance_along(
    rows: List[Dict],
    *,
    x_col: str,
    y_col: str,
) -> List[Dict]:
    """
    Computes cumulative distance_along for a traverse.

    Rules:
    - Uses row order as traverse order
    - First row has distance_along = 0
    - distance_along is always recomputed
    """

    distance = 0.0
    prev_x = None
    prev_y = None

    for r in rows:
        x = float(r[x_col])
        y = float(r[y_col])

        if prev_x is not None:
            distance += hypot(x - prev_x, y - prev_y)

        r["distance_along"] = distance

        prev_x = x
        prev_y = y

    return rows


# ==================================================
# Sparse geometry
# ==================================================
def generate_sparse_geometry(
    rows: List[Dict],
    *,
    spacing: float,
    value_col: str,
) -> List[Dict]:
    """
    Generates uniformly spaced unmeasured stations along a traverse.

    Assumptions:
    - rows already contain distance_along
    - rows are measured points only
    - rows are ordered by distance_along
    """

    if spacing <= 0:
        raise ValueError("spacing must be > 0")

    # Ensure ordering
    rows = sorted(rows, key=lambda r: r["distance_along"])

    out: List[Dict] = []

    max_d = rows[-1]["distance_along"]
    d = 0.0
    idx = 0

    while d <= max_d:
        # Advance to surrounding measured points
        while (
            idx + 1 < len(rows)
            and rows[idx + 1]["distance_along"] < d
        ):
            idx += 1

        # Exact measured station
        if rows[idx]["distance_along"] == d:
            out.append(rows[idx])
        else:
            out.append(
                {
                    "distance_along": d,
                    value_col: "",
                    "is_measured": False,
                }
            )

        d += spacing

    return out
