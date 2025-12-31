from typing import List, Dict
import math


# ============================================================
# Helpers
# ============================================================

def _euclidean_distance(x1, y1, x2, y2) -> float:
    return math.hypot(x2 - x1, y2 - y1)


# ============================================================
# Distance-along-traverse computation
# ============================================================

def compute_distance_along_traverse(
    rows: List[Dict],
    x_col: str,
    y_col: str,
) -> List[Dict]:
    """
    Computes cumulative distance along a traverse.

    Behavior:
    - Orders points along the dominant axis
    - Computes cumulative Euclidean distance
    - Adds 'distance_along' to every row

    Does NOT:
    - assume uniform spacing
    - generate or remove rows
    - touch magnetic values
    """

    if len(rows) < 2:
        raise ValueError("At least two points are required to define a traverse")

    # Cast coordinates
    for r in rows:
        r[x_col] = float(r[x_col])
        r[y_col] = float(r[y_col])

    xs = [r[x_col] for r in rows]
    ys = [r[y_col] for r in rows]

    range_x = max(xs) - min(xs)
    range_y = max(ys) - min(ys)

    dominant_axis = x_col if range_x >= range_y else y_col

    rows_sorted = sorted(rows, key=lambda r: r[dominant_axis])

    distance = 0.0
    rows_sorted[0]["distance_along"] = distance

    for i in range(1, len(rows_sorted)):
        prev = rows_sorted[i - 1]
        curr = rows_sorted[i]

        d = _euclidean_distance(
            prev[x_col], prev[y_col],
            curr[x_col], curr[y_col],
        )

        distance += d
        curr["distance_along"] = distance

    return rows_sorted


# ============================================================
# Sparse geometry generation
# ============================================================

def generate_sparse_geometry(
    rows: List[Dict],
    spacing: float,
    tolerance: float = 1e-6,
) -> List[Dict]:
    """
    Generates missing stations for sparse geometry.

    Rules:
    - Uses distance_along as the 1D axis
    - Preserves all measured rows exactly
    - Generates new rows only at missing distances
    - Generated rows contain ONLY distance_along
    - Magnetic values are intentionally absent

    Geometry only. No train/predict logic here.
    """

    if spacing <= 0:
        raise ValueError("spacing must be positive")

    distances = [float(r["distance_along"]) for r in rows]

    min_d = min(distances)
    max_d = max(distances)

    # Build regular distance grid
    grid = []
    d = min_d
    while d <= max_d + tolerance:
        grid.append(round(d, 6))
        d += spacing

    existing = set(round(d, 6) for d in distances)

    generated_rows = []
    for d in grid:
        if d not in existing:
            generated_rows.append({
                "distance_along": d
            })

    combined = rows + generated_rows
    combined.sort(key=lambda r: float(r["distance_along"]))

    return combined
