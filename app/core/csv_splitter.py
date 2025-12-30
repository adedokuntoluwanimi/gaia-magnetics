import csv
from typing import List, Dict, Optional


Row = Dict[str, Optional[float]]


def read_csv(
    path: str,
    x_col: str,
    y_col: str,
    value_col: Optional[str],
) -> List[Row]:
    """
    Read CSV and normalize rows.

    Returns rows with keys:
    - x: float
    - y: float
    - value: float | None

    Rules:
    - Empty strings → None
    - Rows missing x or y → dropped
    """

    rows: List[Row] = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for i, r in enumerate(reader, start=2):
            try:
                x_raw = r.get(x_col, "").strip()
                y_raw = r.get(y_col, "").strip()

                if not x_raw or not y_raw:
                    continue

                x = float(x_raw)
                y = float(y_raw)

                value: Optional[float] = None
                if value_col:
                    v_raw = r.get(value_col, "").strip()
                    if v_raw != "":
                        value = float(v_raw)

                rows.append(
                    {
                        "x": x,
                        "y": y,
                        "value": value,
                    }
                )

            except ValueError:
                # Drop malformed rows silently
                continue

    return rows
