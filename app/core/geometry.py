# app/core/geometry.py

from dataclasses import dataclass
from typing import List, Optional
from math import hypot


# -------------------------------------------------
# Canonical internal row
# -------------------------------------------------

@dataclass
class GeometryRow:
    x: float
    y: float
    d_along: float
    tmi: Optional[float]
    is_measured: int


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def compute_d_along(points: List[dict]) -> List[float]:
    """
    Compute cumulative distance along traverse.
    Uses row order. No sorting.
    """
    d_along = [0.0]

    for i in range(1, len(points)):
        dx = points[i]["x"] - points[i - 1]["x"]
        dy = points[i]["y"] - points[i - 1]["y"]
        d_along.append(d_along[-1] + hypot(dx, dy))

    return d_along


# -------------------------------------------------
# Geometry builder
# -------------------------------------------------

def build_geometry(
    rows: List[dict],
    scenario: str,
    station_spacing: Optional[float] = None,
) -> List[GeometryRow]:
    """
    Build canonical geometry rows from parsed CSV rows.

    rows: [{x, y, tmi}]
    scenario: 'explicit' or 'sparse'
    """

    if scenario not in {"explicit", "sparse"}:
        raise ValueError(f"Invalid scenario: {scenario}")

    if scenario == "sparse" and station_spacing is None:
        raise ValueError("station_spacing is required for sparse scenario")

    # -------------------------------------------------
    # Explicit geometry
    # -------------------------------------------------
    if scenario == "explicit":
        d_values = compute_d_along(rows)
        output: List[GeometryRow] = []

        for row, d in zip(rows, d_values):
            has_value = row["tmi"] is not None

            output.append(
                GeometryRow(
                    x=row["x"],
                    y=row["y"],
                    d_along=d,
                    tmi=row["tmi"],
                    is_measured=1 if has_value else 0,
                )
            )

        return output

    # -------------------------------------------------
    # Sparse geometry
    # -------------------------------------------------
    # Step 1: compute d_along for original measured points
    original_d = compute_d_along(rows)

    measured_points = []
    for row, d in zip(rows, original_d):
        measured_points.append(
            {
                "x": row["x"],
                "y": row["y"],
                "d_along": d,
                "tmi": row["tmi"],
            }
        )

    total_length = measured_points[-1]["d_along"]

    # Step 2: target d_along positions
    target_d = []
    d = 0.0
    while d < total_length:
        target_d.append(d)
        d += station_spacing
    target_d.append(total_length)

    # Step 3: generate points along polyline
    output: List[GeometryRow] = []

    seg_idx = 0

    for d in target_d:
        # advance segment if needed
        while (
            seg_idx < len(measured_points) - 2
            and measured_points[seg_idx + 1]["d_along"] < d
        ):
            seg_idx += 1

        p0 = measured_points[seg_idx]
        p1 = measured_points[seg_idx + 1]

        d0 = p0["d_along"]
        d1 = p1["d_along"]

        if d1 == d0:
            t = 0.0
        else:
            t = (d - d0) / (d1 - d0)

        x = p0["x"] + t * (p1["x"] - p0["x"])
        y = p0["y"] + t * (p1["y"] - p0["y"])

        # check if this matches a measured point
        is_measured = (
            abs(d - p0["d_along"]) < 1e-6
            or abs(d - p1["d_along"]) < 1e-6
        )

        tmi = None
        if is_measured:
            if abs(d - p0["d_along"]) < 1e-6:
                tmi = p0["tmi"]
            else:
                tmi = p1["tmi"]

        output.append(
            GeometryRow(
                x=x,
                y=y,
                d_along=d,
                tmi=tmi,
                is_measured=1 if is_measured else 0,
            )
        )

    return output

