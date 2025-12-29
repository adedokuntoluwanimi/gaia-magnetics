import json
import tempfile
import csv
from datetime import datetime
from typing import Dict
from typing import Dict, Optional


from app.core.csv_splitter import (
    read_csv,
    split_explicit_geometry,
    split_sparse_geometry,
)
from app.core.s3_io import S3IO
from app.core.models import JobStatus


class JobRunner:
    def __init__(self):
        self.s3 = S3IO()

    def run(
        self,
        job_id: str,
        scenario: str,
        csv_path: str,
        x_col: str,
        y_col: str,
        tmi_col: str,
        station_spacing: Optional[float],
    ) -> Dict:
        # ----------------------------------
        # 1. Read CSV
        # ----------------------------------
        rows = read_csv(
            file_path=csv_path,
            x_col=x_col,
            y_col=y_col,
            tmi_col=tmi_col,
        )

        # ----------------------------------
        # 2. Split geometry
        # ----------------------------------
        if scenario == "explicit":
            train_rows, predict_rows = split_explicit_geometry(rows)

        elif scenario == "sparse":
            if station_spacing is None:
                raise ValueError("station_spacing required for sparse geometry")

            train_rows, predict_rows = split_sparse_geometry(
                rows=rows,
                spacing=station_spacing,
            )

        else:
            raise ValueError(f"Unknown scenario: {scenario}")

        # ----------------------------------
        # 3. Write files locally (temp)
        # ----------------------------------
        with tempfile.TemporaryDirectory() as tmp:
            original_path = f"{tmp}/original.csv"
            train_path = f"{tmp}/train.csv"
            predict_path = f"{tmp}/predict.csv"
            metadata_path = f"{tmp}/metadata.json"

            self._write_original(csv_path, original_path)
            self._write_train(train_rows, train_path)
            self._write_predict(predict_rows, predict_path)

            metadata = {
                "job_id": job_id,
                "scenario": scenario,
                "station_spacing": station_spacing,
                "train_count": len(train_rows),
                "predict_count": len(predict_rows),
                "status": JobStatus.SUBMITTED,
                "created_at": datetime.utcnow().isoformat(),
            }

            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)

            # ----------------------------------
            # 4. Upload to S3
            # ----------------------------------
            self.s3.upload_file(
                original_path,
                self.s3.input_path(job_id, "original.csv"),
            )
            self.s3.upload_file(
                train_path,
                self.s3.split_path(job_id, "train.csv"),
            )
            self.s3.upload_file(
                predict_path,
                self.s3.split_path(job_id, "predict.csv"),
            )
            self.s3.upload_file(
                metadata_path,
                f"{self.s3.job_prefix(job_id)}metadata.json",
            )

        return {
            "status": JobStatus.SUBMITTED,
            "train_count": len(train_rows),
            "predict_count": len(predict_rows),
        }

    # ----------------------------------
    # Writers
    # ----------------------------------
    def _write_original(self, src: str, dst: str) -> None:
        with open(src, "r", encoding="utf-8") as f_src, open(
            dst, "w", encoding="utf-8"
        ) as f_dst:
            f_dst.write(f_src.read())

    def _write_train(self, rows, path: str) -> None:
        if not rows:
            return

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["x", "y", "tmi", "d_along"],
            )
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

    def _write_predict(self, rows, path: str) -> None:
        if not rows:
            return

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["x", "y", "d_along"],
            )
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
