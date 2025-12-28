from math import hypot
from typing import List, Tuple


def compute_distance(
    p1: Tuple[float, float],
    p2: Tuple[float, float],
) -> float:
    return hypot(p2[0] - p1[0], p2[1] - p1[1])


def compute_d_along(
    points: List[Tuple[float, float]],
) -> List[float]:
    """
    Compute cumulative distance along a survey line.
    """
    d_along = [0.0]

    for i in range(1, len(points)):
        d = compute_distance(points[i - 1], points[i])
        d_along.append(d_along[-1] + d)

    return d_along


def generate_station_positions(
    start_d: float,
    end_d: float,
    spacing: float,
) -> List[float]:
    """
    Generate station positions using user-defined spacing.
    """
    positions = []
    d = start_d

    while d <= end_d:
        positions.append(round(d, 6))
        d += spacing

    return positions
