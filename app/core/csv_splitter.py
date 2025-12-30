import csv
from typing import List, Dict, Tuple


def read_and_split_csv(
    csv_path: str,
    x_col: str,
    y_col: str,
    value_col: str,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Returns:
        measured_rows: rows with value present
        predict_rows: rows with missing value

    Each row contains:
        x, y, value (None if missing)
    """

    measured_rows: List[Dict] = []
    predict_rows: List[Dict] = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        required = {x_col, y_col, value_col}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(
                f"CSV missing required columns: {required}"
            )

        for i, r in enumerate(reader, start=2):
            try:
                x = float(r[x_col])
                y = float(r[y_col])
            except (TypeError, ValueError):
                raise ValueError(
                    f"Invalid x/y at line {i}"
                )

            raw_value = r[value_col]

            if raw_value is None or raw_value.strip() == "":
                predict_rows.append({
                    "x": x,
                    "y": y,
                    "value": None,
                })
            else:
                try:
                    value = float(raw_value)
                except ValueError:
                    raise ValueError(
                        f"Invalid value at line {i}"
                    )

                measured_rows.append({
                    "x": x,
                    "y": y,
                    "value": value,
                })

    if not measured_rows:
        raise RuntimeError(
            "No measured rows found in CSV"
        )

    return measured_rows, predict_rows
