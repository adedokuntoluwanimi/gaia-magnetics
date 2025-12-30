import boto3
from pathlib import Path


class S3IO:
    def __init__(self, bucket: str, region: str = "us-east-1"):
        self.bucket = bucket
        self.s3 = boto3.client("s3", region_name=region)

    # -----------------------------
    # Upload
    # -----------------------------
    def upload_file(self, local_path: Path, s3_key: str):
        if not local_path.exists():
            raise FileNotFoundError(local_path)

        self.s3.upload_file(
            str(local_path),
            self.bucket,
            s3_key,
        )

    # -----------------------------
    # Download
    # -----------------------------
    def download_file(self, s3_key: str, local_path: Path):
        local_path.parent.mkdir(parents=True, exist_ok=True)

        self.s3.download_file(
            self.bucket,
            s3_key,
            str(local_path),
        )

    # -----------------------------
    # List objects
    # -----------------------------
    def list_prefix(self, prefix: str):
        paginator = self.s3.get_paginator("list_objects_v2")

        keys = []
        for page in paginator.paginate(
            Bucket=self.bucket,
            Prefix=prefix,
        ):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

        return keys
