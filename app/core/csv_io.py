# app/core/csv_io.py

import csv
from typing import List, Dict, Optional, IO


def read_csv_rows(
    file_obj: IO[str],
    x_column: str,
    y_column: str,
    tmi_column: str,
) -> List[Dict[str, Optional[float]]]:
    """
    Read CSV and extract canonical rows.

    Returns:
        [{ "x": float, "y": float, "tmi": float | None }]
    """

    reader = csv.DictReader(file_obj)

    required = {x_column, y_column, tmi_column}
    missing = required - set(reader.fieldnames or [])

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    rows: List[Dict[str, Optional[float]]] = []

    for i, r in enumerate(reader, start=1):
        try:
            x = float(r[x_column])
            y = float(r[y_column])
        except (TypeError, ValueError):
            raise ValueError(f"Invalid coordinates at row {i}")

        raw_tmi = r.get(tmi_column)

        if raw_tmi is None or raw_tmi.strip() == "":
            tmi = None
        else:
            try:
                tmi = float(raw_tmi)
            except ValueError:
                raise ValueError(f"Invalid TMI value at row {i}")

        rows.append(
            {
                "x": x,
                "y": y,
                "tmi": tmi,
            }
        )

    if not rows:
        raise ValueError("CSV contains no data rows")

    return rows
