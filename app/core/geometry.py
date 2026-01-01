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
    - distance_along is ALWAYS recomputed
    """

    distance = 0.0
    prev_x = None
    prev_y = None

    for r in rows:
        x = float(r[x_col])
        y = float(r[y_col])

        if prev_x is not None:
            distance += hypot(x - prev_x, y - prev_y)

        r["distance_along"] = round(distance, 6)

        prev_x = x
        prev_y = y

    return rows


# ==================================================
# Sparse geometry (CORRECT)
# ==================================================
def generate_sparse_geometry(
    measured_rows: List[Dict],
    *,
    spacing: float,
    value_col: str,
) -> List[Dict]:
    """
    Generates uniformly spaced unmeasured stations while
    preserving ALL measured stations.

    Guarantees:
    - No measured station is ever dropped
    - Generated stations fill gaps BETWEEN measured stations
    - distance_along is the single merge key
    """

    if spacing <= 0:
        raise ValueError("spacing must be > 0")

    if len(measured_rows) < 2:
        raise ValueError("At least two measured points are required")

    # Ensure measured rows are ordered
    measured = sorted(
        measured_rows,
        key=lambda r: r["distance_along"],
    )

    out: List[Dict] = []

    for i in range(len(measured) - 1):
        left = measured[i]
        right = measured[i + 1]

        d_left = left["distance_along"]
        d_right = right["distance_along"]

        # Always keep the left measured point
        out.append(left)

        # Fill gap with synthetic points
        d = d_left + spacing
        while d < d_right:
            out.append(
                {
                    "distance_along": round(d, 6),
                    value_col: "",
                    "is_measured": False,
                }
            )
            d += spacing

    # Always keep the final measured point
    out.append(measured[-1])

    return out
