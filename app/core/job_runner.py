import csv
import json
import tempfile
from datetime import datetime
from typing import Dict, Optional, List

from app.core.csv_splitter import (
    read_csv,
    split_explicit_geometry,
    split_sparse_geometry,
)
from app.core.s3_io import S3IO
from app.core.inference import SageMakerInference
from app.core.merge import (
    merge_measured_and_predicted,
    read_predictions_csv,
    write_final_csv,
)


class JobRunner:
    """
    Orchestrates a single GAIA Magnetics job.

    Assumptions (LOCKED):
    - One CSV = one traverse
    - Geometry is 1D along traverse
    """

    def __init__(self):
        self.s3 = S3IO()
        self.bucket = self.s3.bucket

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

        # --------------------------------------------------
        # 1. Read uploaded CSV
        # --------------------------------------------------
        rows = read_csv(
            file_path=csv_path,
            x_col=x_col,
            y_col=y_col,
            tmi_col=tmi_col,
        )

        # --------------------------------------------------
        # 2. Split geometry
        # --------------------------------------------------
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

        # --------------------------------------------------
        # 3. Write temporary CSVs and upload to S3
        # --------------------------------------------------
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
                "created_at": datetime.utcnow().isoformat(),
            }

            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)

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

        # --------------------------------------------------
        # 4. Inference (predict only)
        # --------------------------------------------------
        inference = SageMakerInference(
            endpoint_name="gaia-xgb-endpoint",
            region="us-east-1",
        )

        inference.run_from_s3(
            bucket=self.bucket,
            predict_key=self.s3.split_path(job_id, "predict.csv"),
            output_key=f"{self.s3.job_prefix(job_id)}predictions/predictions.csv",
        )

        # --------------------------------------------------
        # 5. Merge results
        # --------------------------------------------------
        with tempfile.TemporaryDirectory() as tmp:
            original_local = f"{tmp}/original.csv"
            predictions_local = f"{tmp}/predictions.csv"
            final_local = f"{tmp}/final.csv"

            self.s3.download_file(
                self.s3.input_path(job_id, "original.csv"),
                original_local,
            )
            self.s3.download_file(
                f"{self.s3.job_prefix(job_id)}predictions/predictions.csv",
                predictions_local,
            )

            original_rows = self._read_original_with_flags(original_local)
            predicted_values = read_predictions_csv(predictions_local)

            final_rows = merge_measured_and_predicted(
                original_rows=original_rows,
                predicted_values=predicted_values,
            )

            write_final_csv(final_rows, final_local)

            self.s3.upload_file(
                final_local,
                f"{self.s3.job_prefix(job_id)}final/final.csv",
            )

        return {
            "job_id": job_id,
            "status": "COMPLETED",
            "train_count": len(train_rows),
            "predict_count": len(predict_rows),
            "final_csv": f"{self.s3.job_prefix(job_id)}final/final.csv",
        }

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------
    def _write_original(self, src: str, dst: str) -> None:
        with open(src, "r", encoding="utf-8") as f_src, open(
            dst, "w", encoding="utf-8"
        ) as f_dst:
            f_dst.write(f_src.read())

    def _write_train(self, rows: List[Dict], path: str) -> None:
        if not rows:
            return
        with open(path, "w", newline="", encoding="utf-8") as f:
            fieldnames = ["x", "y", "d_along", "tmi"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                clean = {k: r[k] for k in fieldnames}
                writer.writerow(clean)

    def _write_predict(self, rows: List[Dict], path: str) -> None:
        if not rows:
            return
        with open(path, "w", newline="", encoding="utf-8") as f:
            fieldnames = ["x", "y", "d_along"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                clean = {k: r[k] for k in fieldnames}
                writer.writerow(clean)

    def _read_original_with_flags(self, path: str) -> List[Dict]:
        """
        Re-read original CSV and compute d_along + is_measured.
        One file = one traverse.
        """
        rows = []
        d_accum = 0.0
        prev_x, prev_y = None, None

        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for r in reader:
                x = float(r["x"])
                y = float(r["y"])

                if prev_x is not None:
                    dx = x - prev_x
                    dy = y - prev_y
                    d_accum += (dx ** 2 + dy ** 2) ** 0.5

                prev_x, prev_y = x, y

                tmi_raw = r.get("tmi")
                is_measured = 1 if tmi_raw not in (None, "", "nan") else 0

                rows.append({
                    "x": x,
                    "y": y,
                    "d_along": d_accum,
                    "tmi": float(tmi_raw) if is_measured else None,
                    "is_measured": is_measured,
                })

        return rows
