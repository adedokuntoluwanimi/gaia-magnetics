# app/core/job_runner.py

import csv
import time
from typing import List, Dict

from app.schemas.job import JobCreateRequest, JobStatus
from app.core.geometry import (
    compute_distance_along_traverse,
    generate_sparse_geometry,
)
from app.core.csv_splitter import split_train_predict
from app.core.s3_io import (
    upload_raw_csv,
    download_csv,
    object_exists,
)
from app.core.job_store import (
    create_job_record,
    update_job_status,
)
from app.core.sagemaker_client import SageMakerClient
from app.core.merge import merge_predictions
from app.core.config import settings


class JobRunner:
    """
    Executes a GAIA job as a strict linear pipeline.
    """

    def __init__(self, job_id: str):
        self.job_id = job_id

    async def run(self, csv_file, request: JobCreateRequest):
        # --------------------------------------------------
        # 0. Create job record
        # --------------------------------------------------
        create_job_record(self.job_id)
        update_job_status(self.job_id, JobStatus.running)

        try:
            # --------------------------------------------------
            # 1. Read and store uploaded CSV
            # --------------------------------------------------
            raw = await csv_file.read()
            upload_raw_csv(
                job_id=self.job_id,
                content=raw,
                filename="input/uploaded.csv",
            )

            rows = self._parse_csv(raw)

            # --------------------------------------------------
            # 2. Distance computation
            # --------------------------------------------------
            rows = compute_distance_along_traverse(
                rows,
                x_col=request.x_column,
                y_col=request.y_column,
            )

            # --------------------------------------------------
            # 3. Geometry handling
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
                for r in rows:
                    r["is_measured"] = (
                        r.get(request.value_column) not in ("", None)
                    )

            # --------------------------------------------------
            # 4. Split train / predict
            # --------------------------------------------------
            train, predict = split_train_predict(
                rows,
                value_col=request.value_column,
            )

            if not train:
                raise ValueError("No measured rows for training")

            if not predict:
                raise ValueError("No rows to predict")

            self._upload_csv("input/train.csv", train)
            self._upload_csv("input/predict.csv", predict)

            # --------------------------------------------------
            # 5. Invoke SageMaker async endpoint
            # --------------------------------------------------
            sm = SageMakerClient()
            bucket = settings.s3_bucket
            job = self.job_id

            sm.invoke_async(
                job_id=job,
                train_s3=f"s3://{bucket}/jobs/{job}/input/train.csv",
                predict_s3=f"s3://{bucket}/jobs/{job}/input/predict.csv",
                output_s3=f"s3://{bucket}/jobs/{job}/inference/predictions.csv",
            )

            # --------------------------------------------------
            # 6. Poll S3 for predictions
            # --------------------------------------------------
            for _ in range(120):  # 10 minutes max
                if object_exists(job, "inference/predictions.csv"):
                    break
                time.sleep(5)
            else:
                raise TimeoutError("SageMaker inference timed out")

            # --------------------------------------------------
            # 7. Merge predictions
            # --------------------------------------------------
            preds_raw = download_csv(
                job,
                "inference/predictions.csv",
            )
            predictions = self._parse_csv(preds_raw)

            final_rows = merge_predictions(
                geometry_rows=rows,
                predictions_rows=predictions,
                value_col=request.value_column,
            )

            self._upload_csv("output/final.csv", final_rows)

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

    def _upload_csv(self, path: str, rows: List[Dict]):
        headers = rows[0].keys()
        lines = [",".join(headers)]
        for r in rows:
            lines.append(",".join(str(r.get(h, "")) for h in headers))

        upload_raw_csv(
            job_id=self.job_id,
            content="\n".join(lines).encode(),
            filename=path,
        )
