import csv
from pathlib import Path
from typing import Dict, List


# --------------------------------------------------
# Low-level helpers
# --------------------------------------------------
def _read_csv(path: Path) -> List[Dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


# --------------------------------------------------
# Core merge logic (unchanged conceptually)
# --------------------------------------------------
def _merge_predictions(
    geometry_rows: List[Dict],
    predictions_rows: List[Dict],
    *,
    value_col: str,
    pred_col: str = "predicted_value",
) -> List[Dict]:
    pred_map = {
        float(r["distance_along"]): r[pred_col]
        for r in predictions_rows
    }

    merged: List[Dict] = []

    for r in geometry_rows:
        row = dict(r)
        d = float(row["distance_along"])

        if not row.get("is_measured", False):
            if d not in pred_map:
                raise ValueError(
                    f"Missing prediction for distance_along={d}"
                )
            row[value_col] = pred_map[d]

        merged.append(row)

    return merged


# --------------------------------------------------
# Stage 6 entry point
# --------------------------------------------------
def merge_job_results(job_id: str) -> None:
    """
    Stage 6:
    - Reads geometry CSV
    - Reads inference/predictions.csv
    - Writes output/final.csv
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

    # Value column is whatever already exists in geometry
    # Assumed consistent across rows
    value_col = next(
        k for k in geometry_rows[0].keys()
        if k not in ("distance_along", "is_measured")
    )

    merged = _merge_predictions(
        geometry_rows,
        predictions_rows,
        value_col=value_col,
    )

    _write_csv(output_path, merged)
