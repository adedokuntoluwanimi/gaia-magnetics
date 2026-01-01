import time
import boto3
from app.core.config import settings


class SageMakerBatchClient:
    def __init__(self):
        self.sm = boto3.client(
            "sagemaker",
            region_name=settings.aws_region,
        )

    def run_batch_transform(
        self,
        *,
        job_id: str,
        input_s3_prefix: str,
        output_s3_prefix: str,
    ) -> None:
        """
        Launches a SageMaker Batch Transform job and blocks until completion.
        """

        transform_job_name = f"gaia-batch-{job_id}"

        self.sm.create_transform_job(
            TransformJobName=transform_job_name,
            ModelName=settings.sagemaker_model_name,
            TransformInput={
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": input_s3_prefix,
                    }
                },
                "ContentType": "text/csv",
                "SplitType": "None",
            },
            TransformOutput={
                "S3OutputPath": output_s3_prefix,
                "Accept": "text/csv",
            },
            TransformResources={
                "InstanceType": "ml.c4.xlarge",
                "InstanceCount": 1,
            },
        )

        self._wait_for_completion(transform_job_name)

    def _wait_for_completion(self, transform_job_name: str) -> None:
        """
        Polls SageMaker until the transform job finishes.
        """
        while True:
            response = self.sm.describe_transform_job(
                TransformJobName=transform_job_name
            )
            status = response["TransformJobStatus"]

            if status == "Completed":
                return

            if status in ("Failed", "Stopped"):
                reason = response.get("FailureReason", "unknown")
                raise RuntimeError(
                    f"Batch transform failed: {status} | {reason}"
                )

            time.sleep(10)
