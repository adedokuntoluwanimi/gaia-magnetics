from typing import List, Dict, Tuple
import math


def compute_d_along(rows: List[Dict]) -> List[Dict]:
    """
    Compute distance along traverse for a single line.

    Assumptions (LOCKED):
    - rows are ordered along the traverse
    - each file represents ONE traverse
    """

    d_accum = 0.0
    prev_x, prev_y = None, None

    for r in rows:
        x = r["x"]
        y = r["y"]

        if prev_x is not None:
            dx = x - prev_x
            dy = y - prev_y
            d_accum += math.hypot(dx, dy)

        r["d_along"] = d_accum
        prev_x, prev_y = x, y

    return rows


def infer_traverse_direction(rows: List[Dict]) -> Tuple[float, float]:
    """
    Infer unit direction vector of the traverse
    using first and last station.
    """

    x0, y0 = rows[0]["x"], rows[0]["y"]
    x1, y1 = rows[-1]["x"], rows[-1]["y"]

    dx = x1 - x0
    dy = y1 - y0
    length = math.hypot(dx, dy)

    if length == 0:
        raise ValueError("Traverse start and end are identical")

    return dx / length, dy / length


def generate_sparse_geometry(
    rows: List[Dict],
    station_spacing: float,
) -> List[Dict]:
    """
    Generate full geometry for sparse mode.

    Input:
    - measured rows with x, y, tmi
    - station_spacing chosen by user

    Output:
    - full list of stations with:
        x, y, d_along, tmi (None for generated), is_measured
    """

    if station_spacing <= 0:
        raise ValueError("station_spacing must be positive")

    # Ensure rows are ordered
    rows = compute_d_along(rows)

    total_length = rows[-1]["d_along"]
    ux, uy = infer_traverse_direction(rows)

    generated = []
    measured_map = {
        round(r["d_along"], 6): r for r in rows
    }

    d = 0.0
    while d <= total_length + 1e-6:
        if round(d, 6) in measured_map:
            r = measured_map[round(d, 6)]
            generated.append({
                "x": r["x"],
                "y": r["y"],
                "d_along": d,
                "tmi": r["tmi"],
                "is_measured": 1,
            })
        else:
            x0, y0 = rows[0]["x"], rows[0]["y"]
            generated.append({
                "x": x0 + ux * d,
                "y": y0 + uy * d,
                "d_along": d,
                "tmi": None,
                "is_measured": 0,
            })

        d += station_spacing

    return generated
