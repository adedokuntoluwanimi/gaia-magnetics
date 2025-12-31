from pathlib import Path
from typing import Optional
import csv
import json

from app.schemas.job import JobCreateRequest, JobStatus
from app.core.geometry import (
    compute_distance_along_traverse,
    generate_sparse_geometry,
)
from app.core.csv_splitter import split_train_predict
from app.core.merge import merge_measured_and_predicted
from app.core.s3_io import upload_raw_csv
from app.core.job_store import update_job_status


class JobRunner:
    """
    Executes a single GAIA job as a strict linear pipeline.

    Order is enforced and non-optional.
    """

    def __init__(self, job_id: str):
        self.job_id = job_id

    async def run(self, csv_file, request: JobCreateRequest):
        try:
            update_job_status(self.job_id, JobStatus.running)

            # --------------------------------------------------
            # 1. Read uploaded CSV into memory
            # --------------------------------------------------
            raw_bytes = await csv_file.read()

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
            # 4. Split train / predict (authoritative)
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
            self._upload_csv("train.csv", train_rows)
            self._upload_csv("predict.csv", predict_rows)

            # --------------------------------------------------
            # 6. Stop here for now (inference later)
            # --------------------------------------------------
            update_job_status(self.job_id, JobStatus.completed)

        except Exception as e:
            update_job_status(self.job_id, JobStatus.failed)
            raise

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------

    def _parse_csv_bytes(self, raw: bytes):
        text = raw.decode("utf-8").splitlines()
        reader = csv.DictReader(text)
        return list(reader)

    def _upload_csv(self, filename: str, rows):
        if not rows:
            return

        output = []
        fieldnames = rows[0].keys()

        output.append(",".join(fieldnames))
        for r in rows:
            output.append(",".join(str(r.get(f, "")) for f in fieldnames))

        csv_bytes = "\n".join(output).encode("utf-8")

        upload_raw_csv(
            job_id=self.job_id,
            content=csv_bytes,
            filename=filename,
        )
