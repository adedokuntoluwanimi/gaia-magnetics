from typing import List, Dict
from math import hypot


def apply_explicit_geometry(rows: List[Dict]) -> List[Dict]:
    """
    Explicit geometry:
    - Coordinates are used exactly as provided
    - Only computes d_along
    """

    d = 0.0
    prev = None

    for r in rows:
        if prev is None:
            r["d_along"] = 0.0
        else:
            d += hypot(r["x"] - prev["x"], r["y"] - prev["y"])
            r["d_along"] = d

        prev = r

    return rows


def apply_sparse_geometry(
    rows: List[Dict],
    station_spacing: float,
) -> List[Dict]:
    """
    Sparse geometry:
    - Input rows are measured points
    - Generates intermediate points between consecutive measured points
    - Uses user-provided station spacing
    """

    if station_spacing <= 0:
        raise ValueError("station_spacing must be > 0")

    output = []
    d_total = 0.0

    for i in range(len(rows) - 1):
        start = rows[i]
        end = rows[i + 1]

        dx = end["x"] - start["x"]
        dy = end["y"] - start["y"]
        segment_len = hypot(dx, dy)

        steps = int(segment_len // station_spacing)

        # Always include the start point
        start_row = start.copy()
        start_row["d_along"] = d_total
        output.append(start_row)

        for s in range(1, steps):
            frac = (s * station_spacing) / segment_len

            x = start["x"] + frac * dx
            y = start["y"] + frac * dy

            d_total += station_spacing

            output.append(
                {
                    "x": x,
                    "y": y,
                    "tmi": None,
                    "is_measured": 0,
                    "d_along": d_total,
                }
            )

        d_total += segment_len - (steps * station_spacing)

    # Add final measured point
    final = rows[-1].copy()
    final["d_along"] = d_total
    output.append(final)

    return output
