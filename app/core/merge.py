from pathlib import Path
import csv
from typing import Dict, List


# ==================================================
# Low-level helpers
# ==================================================
def _read_csv(path: Path) -> List[Dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


# ==================================================
# Core merge logic
# ==================================================
def merge_job_results(job_id: str) -> None:
    """
    Stage 6:
    - Reads frozen geometry
    - Reads predictions
    - Merges predicted values into unmeasured rows
    - Writes full traverse to output/final.csv
    """

    base = Path("data") / job_id

    geometry_path = base / "geometry" / "geometry.csv"
    predictions_path = base / "inference" / "predictions.csv"
    output_path = base / "output" / "final.csv"

    if not geometry_path.exists():
        raise FileNotFoundError(f"Missing geometry file: {geometry_path}")

    if not predictions_path.exists():
        raise FileNotFoundError(
            f"Missing predictions file: {predictions_path}"
        )

    geometry_rows = _read_csv(geometry_path)
    predictions_rows = _read_csv(predictions_path)

    if not geometry_rows:
        raise RuntimeError("Geometry CSV is empty")

    if not predictions_rows:
        raise RuntimeError("Predictions CSV is empty")

    # --------------------------------------------------
    # Validate prediction schema
    # --------------------------------------------------
    for col in ("distance_along", "predicted_value"):
        if col not in predictions_rows[0]:
            raise RuntimeError(
                f"predictions.csv missing required column '{col}'"
            )

    # --------------------------------------------------
    # Determine value column from geometry
    # --------------------------------------------------
    value_cols = [
        c for c in geometry_rows[0].keys()
        if c not in ("distance_along", "is_measured")
    ]

    if len(value_cols) != 1:
        raise RuntimeError(
            f"Expected exactly one value column in geometry, found {value_cols}"
        )

    value_col = value_cols[0]

    # --------------------------------------------------
    # Build prediction lookup
    # --------------------------------------------------
    pred_map = {
        float(r["distance_along"]): r["predicted_value"]
        for r in predictions_rows
    }

    # --------------------------------------------------
    # Merge
    # --------------------------------------------------
    merged: List[Dict] = []

    for r in geometry_rows:
        row = dict(r)
        d = float(row["distance_along"])

        if not row["is_measured"]:
            if d not in pred_map:
                raise RuntimeError(
                    f"Missing prediction for distance_along={d}"
                )
            row[value_col] = pred_map[d]

        merged.append(row)

    _write_csv(output_path, merged)
