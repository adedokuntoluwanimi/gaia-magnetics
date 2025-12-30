# app/core/job_runner.py

import uuid
from typing import Optional, List

from app.core.csv_io import read_csv_rows
from app.core.geometry import build_geometry, GeometryRow
from app.core.split import split_train_predict
from app.core.merge import merge_predictions
from app.core.s3_io import S3IO
from app.core.inference import SageMakerInference


class JobRunner:
    """
    Orchestrates a single GAIA job.

    Guarantees:
    - One CSV = one traverse
    - Deterministic geometry
    - Deterministic split
    - Train-on-the-fly inference
    - Deterministic merge
    """

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_region: Optional[str],
        sagemaker_endpoint: str,
    ):
        self.s3 = S3IO(
            bucket=s3_bucket,
            region=s3_region,
        )

        self.inference = SageMakerInference(
            endpoint_name=sagemaker_endpoint,
            region=s3_region,
        )

    def run(
        self,
        *,
        csv_file,
        scenario: str,
        x_column: str,
        y_column: str,
        tmi_column: str,
        station_spacing: Optional[float] = None,
    ) -> str:
        """
        Execute full backend pipeline.

        Returns:
            job_id (str)
        """

        job_id = f"job-{uuid.uuid4().hex}"

        # -------------------------------------------------
        # 1. Read CSV
        # -------------------------------------------------
        rows = read_csv_rows(
            file_obj=csv_file,
            x_column=x_column,
            y_column=y_column,
            tmi_column=tmi_column,
        )

        # -------------------------------------------------
        # 2. Build geometry
        # -------------------------------------------------
        geometry: List[GeometryRow] = build_geometry(
            rows=rows,
            scenario=scenario,
            station_spacing=station_spacing,
        )

        # -------------------------------------------------
        # 3. Split train / predict
        # -------------------------------------------------
        train_rows, predict_rows = split_train_predict(geometry)

        # -------------------------------------------------
        # 4. Upload train & predict CSVs
        # -------------------------------------------------
        train_key = f"{job_id}/train.csv"
        predict_key = f"{job_id}/predict.csv"

        self.s3.upload_train_csv(
            key=train_key,
            rows=train_rows,
        )

        self.s3.upload_predict_csv(
            key=predict_key,
            rows=predict_rows,
        )

        # -------------------------------------------------
        # 5. Train-on-the-fly inference
        # -------------------------------------------------
        train_s3_uri = f"s3://{self.s3.bucket}/{train_key}"
        predict_s3_uri = f"s3://{self.s3.bucket}/{predict_key}"

        predictions = self.inference.predict(
            train_s3_uri=train_s3_uri,
            predict_s3_uri=predict_s3_uri,
        )

        # -------------------------------------------------
        # 6. Merge predictions
        # -------------------------------------------------
        final_rows = merge_predictions(
            geometry=geometry,
            predictions=predictions,
        )

        # -------------------------------------------------
        # 7. Upload final CSV
        # -------------------------------------------------
        final_key = f"{job_id}/final.csv"
        self._upload_final_csv(
            key=final_key,
            rows=final_rows,
        )

        return job_id

    # -------------------------------------------------
    # Internal helpers
    # -------------------------------------------------

    def _upload_final_csv(
        self,
        *,
        key: str,
        rows: List[GeometryRow],
    ) -> None:
        """
        Upload merged final CSV (no header).
        """
        import csv
        import io

        buf = io.StringIO()
        writer = csv.writer(buf)

        for r in rows:
            writer.writerow(
                [
                    r.x,
                    r.y,
                    r.d_along,
                    r.tmi,
                    r.is_measured,
                ]
            )

        self.s3._upload_text(key, buf.getvalue())
