import os
import boto3
from botocore.exceptions import ClientError


class S3IO:
    def __init__(self):
        self.bucket = os.environ.get("GAIA_S3_BUCKET")
        if not self.bucket:
            raise RuntimeError("GAIA_S3_BUCKET environment variable not set")

        self.client = boto3.client("s3")

    # -----------------------------
    # Path helpers
    # -----------------------------
    def job_prefix(self, job_id: str) -> str:
        return f"gaia/jobs/{job_id}/"

    def input_path(self, job_id: str, filename: str) -> str:
        return f"{self.job_prefix(job_id)}input/{filename}"

    def split_path(self, job_id: str, filename: str) -> str:
        return f"{self.job_prefix(job_id)}split/{filename}"

    def inference_path(self, job_id: str, filename: str) -> str:
        return f"{self.job_prefix(job_id)}inference/{filename}"

    def output_path(self, job_id: str, filename: str) -> str:
        return f"{self.job_prefix(job_id)}output/{filename}"

    # -----------------------------
    # Core operations
    # -----------------------------
    def upload_file(self, local_path: str, s3_key: str) -> None:
        self.client.upload_file(
            Filename=local_path,
            Bucket=self.bucket,
            Key=s3_key,
        )

    def download_file(self, s3_key: str, local_path: str) -> None:
        self.client.download_file(
            Bucket=self.bucket,
            Key=s3_key,
            Filename=local_path,
        )

    def exists(self, s3_key: str) -> bool:
        try:
            self.client.head_object(
                Bucket=self.bucket,
                Key=s3_key,
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise
