import boto3
from pathlib import Path
from app.core.config import settings


class S3IO:
    def __init__(self):
        self.bucket = settings.s3_bucket
        self.s3 = boto3.client(
            "s3",
            region_name=settings.aws_region,
        )

    # --------------------------------------------------
    # Existing functionality (unchanged)
    # --------------------------------------------------
    def upload_raw_csv(self, job_id: str, content: bytes, filename: str) -> str:
        key = f"jobs/{job_id}/input/{filename}"

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content,
            ContentType="text/csv",
        )

        return key

    def download_csv(self, job_id: str, filename: str) -> bytes:
        key = f"jobs/{job_id}/{filename}"
        response = self.s3.get_object(
            Bucket=self.bucket,
            Key=key,
        )
        return response["Body"].read()

    def object_exists(self, job_id: str, filename: str) -> bool:
        try:
            self.s3.head_object(
                Bucket=self.bucket,
                Key=f"jobs/{job_id}/{filename}",
            )
            return True
        except Exception:
            return False

    # --------------------------------------------------
    # New: required for Batch Transform output
    # --------------------------------------------------
    def download_prefix(self, prefix: str, local_dir: Path) -> None:
        """
        Downloads all objects under an S3 prefix into a local directory.
        """
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(
            Bucket=self.bucket,
            Prefix=prefix,
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                if key.endswith("/"):
                    continue

                relative_path = key[len(prefix):]
                local_path = local_dir / relative_path
                local_path.parent.mkdir(parents=True, exist_ok=True)

                self.s3.download_file(
                    self.bucket,
                    key,
                    str(local_path),
                )
