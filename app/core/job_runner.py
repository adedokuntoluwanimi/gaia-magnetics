import csv
from typing import List, Dict

from app.schemas.job import JobCreateRequest, JobStatus
from app.core.geometry import (
    compute_distance_along_traverse,
    generate_sparse_geometry,
)
from app.core.csv_splitter import split_train_predict
from app.core.s3_io import upload_raw_csv
from app.core.job_store import create_job_record, update_job_status


class JobRunner:
    """
    Executes a single GAIA job as a strict, linear pipeline.

    Contract:
    - Always creates a job record first
    - Always materializes train.csv and predict.csv
    - Geometry is resolved before splitting
    """

    def __init__(self, job_id: str):
        self.job_id = job_id

    async def run(self, csv_file, request: JobCreateRequest):
        try:
            # --------------------------------------------------
            # 0. Create job record immediately
            # --------------------------------------------------
            create_job_record(self.job_id)

            # --------------------------------------------------
            # 1. Read uploaded CSV safely
            # --------------------------------------------------
            raw_bytes = await csv_file.read()
            update_job_status(self.job_id, JobStatus.running)

            upload_raw_csv(
                job_id=self.job_id,
                content=raw_bytes,
                filename="uploaded.csv",
            )

            rows = self._parse_csv_bytes(raw_bytes)

            # --------------------------------------------------
            # 2. Compute distance along traverse (always)
            # --------------------------------------------------
            rows = compute_distance_along_traverse(
                rows,
                x_col=request.x_column,
                y_col=request.y_column,
            )

            # --------------------------------------------------
            # 3. Scenario-specific geometry
            # --------------------------------------------------
            if request.scenario == "sparse":
                rows = generate_sparse_geometry(
                    rows,
                    spacing=request.station_spacing,
                )

            # --------------------------------------------------
            # 4. Split train / predict (value-based only)
            # --------------------------------------------------
            train_rows, predict_rows = split_train_predict(
                rows,
                value_col=request.value_column,
            )

            if not train_rows:
                raise ValueError("No measured rows found for training")

            if not predict_rows:
                raise ValueError("No rows require prediction")

            # --------------------------------------------------
            # 5. Upload train / predict to S3
            # --------------------------------------------------
            self._upload_csv(
                filename="train.csv",
                rows=train_rows,
            )

            self._upload_csv(
                filename="predict.csv",
                rows=predict_rows,
            )

            # --------------------------------------------------
            # 6. Mark job complete
            # --------------------------------------------------
            update_job_status(self.job_id, JobStatus.completed)

        except Exception:
            update_job_status(self.job_id, JobStatus.failed)
            raise

    # ==================================================
    # Helpers
    # ==================================================

    def _parse_csv_bytes(self, raw: bytes) -> List[Dict]:
        text = raw.decode("utf-8").splitlines()
        reader = csv.DictReader(text)
        return list(reader)

    def _upload_csv(self, filename: str, rows: List[Dict]):
        """
        Writes rows to CSV with a stable column order.
        """
        if not rows:
            return

        # Stable column order
        fieldnames = list(rows[0].keys())

        output_lines = []
        output_lines.append(",".join(fieldnames))

        for row in rows:
            output_lines.append(
                ",".join(str(row.get(f, "")) for f in fieldnames)
            )

        csv_bytes = "\n".join(output_lines).encode("utf-8")

        upload_raw_csv(
            job_id=self.job_id,
            content=csv_bytes,
            filename=filename,
        )
