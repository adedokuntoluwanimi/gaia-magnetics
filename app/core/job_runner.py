from pathlib import Path
import csv

from fastapi import UploadFile

from app.core.s3_io import S3IO
from app.core.sagemaker_client import SageMakerBatchClient
from app.core.merge import merge_job_results
from app.core.csv_splitter import split_train_predict
from app.schemas.job import JobCreateRequest


class JobRunner:
    """
    Orchestrates a single GAIA Magnetics job.

    Stage 1–2: prepare (save, split, upload)
    Stage 3–6: batch transform + merge
    """

    def __init__(self):
        self.s3 = S3IO()
        self.sm = SageMakerBatchClient()

    # --------------------------------------------------
    # Stage 1–2
    # --------------------------------------------------
    async def prepare(
        self,
        job_id: str,
        csv_file: UploadFile,
        request: JobCreateRequest,
    ) -> None:
        base = Path("data") / job_id
        input_dir = base / "input"
        geometry_dir = base / "geometry"

        input_dir.mkdir(parents=True, exist_ok=True)
        geometry_dir.mkdir(parents=True, exist_ok=True)

        # Save uploaded CSV
        uploaded_path = input_dir / "uploaded.csv"
        content = await csv_file.read()
        uploaded_path.write_bytes(content)

        # Read rows
        with open(uploaded_path, newline="") as f:
            rows = list(csv.DictReader(f))

        if not rows:
            raise RuntimeError("Uploaded CSV is empty")

        # Split train / predict
        train_rows, predict_rows = split_train_predict(
            rows,
            value_col=request.value_column,
        )

        # Persist geometry (full rows)
        geometry_path = geometry_dir / "geometry.csv"
        self._write_csv(geometry_path, rows)

        # Persist train / predict
        train_path = input_dir / "train" / "train.csv"
        predict_path = input_dir / "predict" / "predict.csv"

        train_path.parent.mkdir(parents=True, exist_ok=True)
        predict_path.parent.mkdir(parents=True, exist_ok=True)

        self._write_csv(train_path, train_rows)
        self._write_csv(predict_path, predict_rows)

        # Upload to S3
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

    # --------------------------------------------------
    # Stage 3–6
    # --------------------------------------------------
    def run(self, job_id: str) -> None:
        input_prefix = f"s3://{self.s3.bucket}/jobs/{job_id}/input/"
        output_prefix = f"s3://{self.s3.bucket}/jobs/{job_id}/raw-output/"

        # Batch Transform
        self.sm.run_batch_transform(
            job_id=job_id,
            input_s3_prefix=input_prefix,
            output_s3_prefix=output_prefix,
        )

        # Normalize + merge
        self._normalize_predictions(job_id)
        merge_job_results(job_id)

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------
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

    def _write_csv(self, path: Path, rows: list[dict]) -> None:
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
