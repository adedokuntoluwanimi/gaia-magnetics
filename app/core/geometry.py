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

    - Orders points by projection along the dominant axis
    - Computes distance incrementally
    - Adds a 'distance_along' field to each row

    This function does NOT:
    - assume uniform spacing
    - modify original coordinates
    """

    if len(rows) < 2:
        raise ValueError("At least two points are required to define a traverse")

    # Cast coordinates to float
    for r in rows:
        r[x_col] = float(r[x_col])
        r[y_col] = float(r[y_col])

    # Determine dominant axis (relative, not absolute)
    xs = [r[x_col] for r in rows]
    ys = [r[y_col] for r in rows]

    range_x = max(xs) - min(xs)
    range_y = max(ys) - min(ys)

    dominant_axis = x_col if range_x >= range_y else y_col

    # Sort rows along dominant axis
    rows_sorted = sorted(rows, key=lambda r: r[dominant_axis])

    # Compute cumulative distance
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
    Generates missing stations along a traverse for sparse geometry.

    Behavior:
    - Uses distance_along as the 1D axis
    - Generates a regular distance grid using spacing
    - Preserves all measured rows exactly
    - Adds new rows ONLY where no measured row exists nearby

    Returns:
    - Combined list of measured + generated rows
    """

    if spacing <= 0:
        raise ValueError("spacing must be positive")

    # Separate measured rows
    measured = rows

    distances = [r["distance_along"] for r in measured]

    min_d = min(distances)
    max_d = max(distances)

    # Generate full distance grid
    generated_distances = []
    d = min_d
    while d <= max_d + tolerance:
        generated_distances.append(round(d, 6))
        d += spacing

    # Identify existing distances
    existing = set(round(d, 6) for d in distances)

    # Generate missing stations
    generated_rows = []
    for d in generated_distances:
        if d not in existing:
            generated_rows.append({
                "distance_along": d,
                "__generated__": True,
            })

    # Mark measured rows explicitly
    for r in measured:
        r["__generated__"] = False

    # Combine and sort
    combined = measured + generated_rows
    combined.sort(key=lambda r: r["distance_along"])

    return combined
