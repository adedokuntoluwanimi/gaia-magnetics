# app/core/s3_io.py

import csv
import io
from typing import List
import boto3

from app.core.geometry import GeometryRow


class S3IO:
    def __init__(self, bucket: str, region: str | None = None):
        self.bucket = bucket
        self.s3 = boto3.client("s3", region_name=region)

    # -------------------------------------------------
    # Upload helpers
    # -------------------------------------------------

    def upload_train_csv(
        self,
        key: str,
        rows: List[GeometryRow],
    ) -> None:
        """
        Upload measured rows as train.csv (no header).
        """
        buf = io.StringIO()
        writer = csv.writer(buf)

        for r in rows:
            writer.writerow([r.x, r.y, r.d_along, r.tmi])

        self._upload_text(key, buf.getvalue())

    def upload_predict_csv(
        self,
        key: str,
        rows: List[GeometryRow],
    ) -> None:
        """
        Upload predict rows as predict.csv (no header).
        """
        buf = io.StringIO()
        writer = csv.writer(buf)

        for r in rows:
            writer.writerow([r.x, r.y, r.d_along])

        self._upload_text(key, buf.getvalue())

    # -------------------------------------------------
    # Download helpers
    # -------------------------------------------------

    def download_predictions(
        self,
        key: str,
    ) -> List[float]:
        """
        Download prediction CSV and return values in order.
        """
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")

        reader = csv.reader(io.StringIO(body))
        predictions: List[float] = []

        for i, row in enumerate(reader, start=1):
            if not row:
                continue
            try:
                predictions.append(float(row[0]))
            except ValueError:
                raise ValueError(f"Invalid prediction value at row {i}")

        if not predictions:
            raise ValueError("Prediction file is empty")

        return predictions

    # -------------------------------------------------
    # Internal
    # -------------------------------------------------

    def _upload_text(self, key: str, text: str) -> None:
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=text.encode("utf-8"),
        )
