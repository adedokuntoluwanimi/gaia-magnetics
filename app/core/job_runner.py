# app/core/job_runner.py

import csv
from typing import List, Dict

from app.schemas.job import JobCreateRequest, JobStatus
from app.core.geometry import (
    compute_distance_along_traverse,
    generate_sparse_geometry,
)
from app.core.csv_splitter import split_train_predict
from app.core.s3_io import upload_raw_csv
from app.core.job_store import (
    create_job_record,
    update_job_status,
)


class JobRunner:
    """
    Executes a GAIA job as a strict linear pipeline.
    """

    def __init__(self, job_id: str):
        self.job_id = job_id

    async def run(self, csv_file, request: JobCreateRequest):
        # --------------------------------------------------
        # 0. Create job record FIRST
        # --------------------------------------------------
        create_job_record(self.job_id)
        update_job_status(self.job_id, JobStatus.running)

        try:
            raw = await csv_file.read()
            upload_raw_csv(self.job_id, raw, "uploaded.csv")

            rows = self._parse_csv(raw)

            # --------------------------------------------------
            # 1. Distance computation (always)
            # --------------------------------------------------
            rows = compute_distance_along_traverse(
                rows,
                x_col=request.x_column,
                y_col=request.y_column,
            )

            # --------------------------------------------------
            # 2. Geometry
            # --------------------------------------------------
            if request.scenario == "sparse":
                rows = generate_sparse_geometry(
                    rows,
                    x_col=request.x_column,
                    y_col=request.y_column,
                    value_col=request.value_column,
                    spacing=request.station_spacing,
                )
            else:
                # explicit geometry
                for r in rows:
                    r["is_measured"] = r.get(request.value_column, "") not in ("", None)

            # --------------------------------------------------
            # 3. Split train / predict
            # --------------------------------------------------
            train, predict = split_train_predict(
                rows,
                value_col=request.value_column,
            )

            if not train:
                raise ValueError("No measured rows for training")

            if not predict:
                raise ValueError("No rows to predict")

            # --------------------------------------------------
            # 4. Upload authoritative CSVs
            # --------------------------------------------------
            self._upload_csv("train.csv", train)
            self._upload_csv("predict.csv", predict)

            update_job_status(self.job_id, JobStatus.completed)

        except Exception:
            update_job_status(self.job_id, JobStatus.failed)
            raise

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------

    def _parse_csv(self, raw: bytes) -> List[Dict]:
        text = raw.decode("utf-8").splitlines()
        return list(csv.DictReader(text))

    def _upload_csv(self, name: str, rows: List[Dict]):
        headers = rows[0].keys()
        lines = [",".join(headers)]
        for r in rows:
            lines.append(",".join(str(r.get(h, "")) for h in headers))

        upload_raw_csv(
            job_id=self.job_id,
            content="\n".join(lines).encode(),
            filename=name,
        )
