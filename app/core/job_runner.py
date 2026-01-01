from pathlib import Path
import csv
from typing import Any, List, Dict

from fastapi import UploadFile

from app.schemas.job import JobCreateRequest
from app.core.s3_io import S3IO
from app.core.sagemaker_client import SageMakerBatchClient
from app.core.csv_splitter import split_train_predict
from app.core.geometry import (
    compute_distance_along,
    generate_sparse_geometry,
)
from app.core.merge import merge_job_results


# ==================================================
# Input normalization (Stage 1 boundary)
# ==================================================
def normalize_is_measured(value: Any) -> bool:
    if value is None:
        raise ValueError("is_measured is missing")

    if isinstance(value, bool):
        return value

    v = str(value).strip().lower()

    if v in {"true", "1", "yes"}:
        return True
    if v in {"false", "0", "no"}:
        return False

    raise ValueError(f"Invalid is_measured value: {value}")


# ==================================================
# JobRunner
# ==================================================
class JobRunner:
    """
    Orchestrates a single GAIA Magnetics job.

    Stage order:
    1. CSV ingestion + normalization
    2. Geometry (explicit or sparse)
    3. Train / predict split
    4. Upload to S3
    5. Batch Transform
    6. Merge
    """

    def __init__(self):
        self.s3 = S3IO()
        self.sm = SageMakerBatchClient()

    # ==================================================
    # Stage 1–4: Prepare job
    # ==================================================
    async def prepare(
        self,
        job_id: str,
        csv_file: UploadFile,
        request: JobCreateRequest,
    ) -> None:

        # -------------------------------
        # Local directory contract
        # -------------------------------
        base = Path("data") / job_id
        geometry_dir = base / "geometry"
        input_dir = base / "input"
        train_dir = input_dir / "train"
        predict_dir = input_dir / "predict"

        geometry_dir.mkdir(parents=True, exist_ok=True)
        train_dir.mkdir(parents=True, exist_ok=True)
        predict_dir.mkdir(parents=True, exist_ok=True)

        # -------------------------------
        # Save uploaded CSV
        # -------------------------------
        uploaded_path = input_dir / "uploaded.csv"
        uploaded_path.write_bytes(await csv_file.read())

        # -------------------------------
        # Read + normalize rows
        # -------------------------------
        rows: List[Dict] = []

        with open(uploaded_path, newline="") as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    row["is_measured"] = normalize_is_measured(
                        row.get("is_measured")
                    )
                except ValueError as e:
                    raise RuntimeError(f"CSV validation error: {e}")

                rows.append(row)

        if not rows:
            raise RuntimeError("Uploaded CSV is empty")

        # -------------------------------
        # Geometry (always computed)
        # -------------------------------
        rows = compute_distance_along(
            rows,
            x_col=request.x_column,
            y_col=request.y_column,
        )

        # -------------------------------
        # Sparse geometry (optional)
        # -------------------------------
        if request.scenario == "sparse":
            measured_rows = [r for r in rows if r["is_measured"]]

            if len(measured_rows) < 2:
                raise RuntimeError(
                    "Sparse geometry requires at least two measured points"
                )

            rows = generate_sparse_geometry(
                measured_rows,
                spacing=request.spacing,
                value_col=request.value_column,
            )

        # -------------------------------
        # Freeze geometry
        # -------------------------------
        geometry_path = geometry_dir / "geometry.csv"
        self._write_csv(geometry_path, rows)

        # -------------------------------
        # Split train / predict
        # -------------------------------
        train_rows, predict_rows = split_train_predict(
            rows,
            value_col=request.value_column,
        )

        if not train_rows:
            raise RuntimeError(
                "No training rows found. "
                "Measured rows must have values."
            )

        if not predict_rows:
            raise RuntimeError(
                "No prediction rows found. "
                "Unmeasured rows must have empty values."
            )

        # -------------------------------
        # Write train / predict locally
        # -------------------------------
        train_path = train_dir / "train.csv"
        predict_path = predict_dir / "predict.csv"

        self._write_csv(train_path, train_rows)
        self._write_csv(predict_path, predict_rows)

        # -------------------------------
        # Upload to S3 (authoritative)
        # -------------------------------
        self.s3.upload_raw_csv(
            job_id,
            train_path.read_bytes(),
            "train/train.csv",
        )

        self.s3.upload_raw_csv(
            job_id,
            predict_path.read_bytes(),
            "predict/predict.csv",
        )

    # ==================================================
    # Stage 5–6: Batch Transform + merge
    # ==================================================
    def run(self, job_id: str) -> None:
        input_prefix = f"s3://{self.s3.bucket}/jobs/{job_id}/input/"
        output_prefix = f"s3://{self.s3.bucket}/jobs/{job_id}/raw-output/"

        # Batch Transform (blocking)
        self.sm.run_batch_transform(
            job_id=job_id,
            input_s3_prefix=input_prefix,
            output_s3_prefix=output_prefix,
        )

        # Download + merge
        self._normalize_predictions(job_id)
        merge_job_results(job_id)

    # ==================================================
    # Helpers
    # ==================================================
    def _normalize_predictions(self, job_id: str) -> None:
        local_dir = Path("data") / job_id / "inference"
        local_dir.mkdir(parents=True, exist_ok=True)

        self.s3.download_prefix(
            f"jobs/{job_id}/raw-output/",
            local_dir,
        )

        csv_files = list(local_dir.glob("*.csv"))
        if not csv_files:
            raise RuntimeError("No predictions returned from Batch Transform")

        final_path = local_dir / "predictions.csv"
        csv_files[0].replace(final_path)

    def _write_csv(self, path: Path, rows: List[Dict]) -> None:
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
