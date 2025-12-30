from pathlib import Path
from typing import Tuple, Optional
import csv
import json

from app.schemas.job import JobCreateRequest, JobStatus
from app.core.geometry import (
    compute_distance_along_traverse,
    generate_sparse_geometry,
)
from app.core.csv_splitter import split_train_predict
from app.core.inference import run_sagemaker_inference
from app.core.merge import merge_measured_and_predicted


BASE_DIR = Path("jobs")  # local job workspace root


class JobRunner:
    """
    Executes a single GAIA job as a linear pipeline.

    Responsibilities:
    - Track job state
    - Execute pipeline steps in order
    - Persist intermediate and final outputs
    """

    def __init__(self, job_id: str):
        self.job_id = job_id
        self.job_dir = BASE_DIR / job_id
        self.status_file = self.job_dir / "status.json"
        self.result_csv = self.job_dir / "final.csv"
        self.result_json = self.job_dir / "final.json"

        self.job_dir.mkdir(parents=True, exist_ok=True)

        if not self.status_file.exists():
            self._write_status(JobStatus.created)

    # ---------------------------------------------------------
    # Public API
    # ---------------------------------------------------------

    async def run(self, csv_file, request: JobCreateRequest):
        """
        Run the full pipeline.
        """

        try:
            self._write_status(JobStatus.running)

            # 1. Save uploaded CSV
            input_csv = self.job_dir / "input.csv"
            with open(input_csv, "wb") as f:
                f.write(await csv_file.read())

            # 2. Parse CSV
            rows = self._read_csv(input_csv)

            # 3. Compute distance-along-traverse
            rows = compute_distance_along_traverse(
                rows,
                x_col=request.x_column,
                y_col=request.y_column,
            )

            # 4. Geometry handling
            if request.scenario == "sparse":
                rows = generate_sparse_geometry(
                    rows,
                    spacing=request.station_spacing,
                )

            # 5. Split train / predict
            train_rows, predict_rows = split_train_predict(
                rows,
                value_col=request.value_column,
            )

            train_csv = self.job_dir / "train.csv"
            predict_csv = self.job_dir / "predict.csv"

            self._write_csv(train_csv, train_rows)
            self._write_csv(predict_csv, predict_rows)

            # 6. SageMaker inference
            predictions = run_sagemaker_inference(
                train_csv=train_csv,
                predict_csv=predict_csv,
            )

            predictions_csv = self.job_dir / "predictions.csv"
            self._write_csv(predictions_csv, predictions)

            # 7. Merge measured + predicted
            final_rows = merge_measured_and_predicted(
                train_rows=train_rows,
                predicted_rows=predictions,
            )

            self._write_csv(self.result_csv, final_rows)
            self._write_json(self.result_json, final_rows)

            self._write_status(JobStatus.completed)

        except Exception as e:
            self._write_status(JobStatus.failed, message=str(e))
            raise

    def get_status(self) -> Tuple[JobStatus, Optional[str]]:
        data = json.loads(self.status_file.read_text())
        return JobStatus(data["status"]), data.get("message")

    def is_completed(self) -> bool:
        status, _ = self.get_status()
        return status == JobStatus.completed

    def get_result_csv_path(self) -> Path:
        return self.result_csv

    def load_result_json(self):
        return json.loads(self.result_json.read_text())

    # ---------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------

    def _write_status(self, status: JobStatus, message: Optional[str] = None):
        payload = {"status": status}
        if message:
            payload["message"] = message
        self.status_file.write_text(json.dumps(payload))

    def _read_csv(self, path: Path):
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def _write_csv(self, path: Path, rows):
        if not rows:
            return
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    def _write_json(self, path: Path, rows):
        path.write_text(json.dumps(rows, indent=2))
