from pathlib import Path
from app.core.s3_io import S3IO
from app.core.sagemaker_client import SageMakerBatchClient
from app.core.merge import merge_job_results


class JobRunner:
    """
    Orchestrates a single GAIA Magnetics job.
    One job = one traverse = one Batch Transform.
    """

    def __init__(self):
        self.s3 = S3IO()
        self.sm = SageMakerBatchClient()

    def run(self, job_id: str) -> None:
        """
        Full execution of a GAIA job.
        """

        # --------------------------------------------------
        # S3 prefixes (avoids string duplication)
        # --------------------------------------------------
        input_prefix = f"s3://{self.s3.bucket}/jobs/{job_id}/input/"
        output_prefix = f"s3://{self.s3.bucket}/jobs/{job_id}/raw-output/"

        # --------------------------------------------------
        # Stage 3–4: Batch Transform
        # --------------------------------------------------
        self.sm.run_batch_transform(
            job_id=job_id,
            input_s3_prefix=input_prefix,
            output_s3_prefix=output_prefix,
        )

        # --------------------------------------------------
        # Stage 5: Normalize predictions
        # --------------------------------------------------
        self._normalize_predictions(job_id)

        # --------------------------------------------------
        # Stage 6: Merge + final output
        # --------------------------------------------------
        merge_job_results(job_id)

    def _normalize_predictions(self, job_id: str) -> None:
        """
        Converts SageMaker output to inference/predictions.csv
        """

        local_dir = Path("data") / job_id / "inference"
        local_dir.mkdir(parents=True, exist_ok=True)

        # SageMaker writes one or more part files
        self.s3.download_prefix(
            f"jobs/{job_id}/raw-output/",
            local_dir,
        )

        part_files = list(local_dir.glob("*.csv"))
        if not part_files:
            raise RuntimeError("No predictions returned from Batch Transform")

        # Deterministic output
        final_path = local_dir / "predictions.csv"
        part_files[0].replace(final_path)
