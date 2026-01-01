import time
import boto3
from app.core.config import settings


class SageMakerClient:
    """
    Handles SageMaker Batch Transform jobs for GAIA Magnetics.
    One job = one traverse = one transform job.
    """

    def __init__(self):
        self.sm = boto3.client(
            "sagemaker",
            region_name=settings.aws_region,
        )

    def run_batch_transform(
        self,
        job_id: str,
        train_s3_prefix: str,
        predict_s3_prefix: str,
        output_s3_prefix: str,
    ):
        """
        Launches a Batch Transform job.

        Expected S3 layout BEFORE job:
          train_s3_prefix/train.csv
          predict_s3_prefix/predict.csv

        Output AFTER job:
          output_s3_prefix/predictions.csv
        """

        transform_job_name = f"{job_id}-transform"

        print("SETTINGS DIR:", dir(settings))
        print("SETTINGS TYPE:", type(settings))
        print("SETTINGS DIR:", dir(settings))



        self.sm.create_transform_job(
            TransformJobName=transform_job_name,
            ModelName=settings.sagemaker_model_name,
            MaxConcurrentTransforms=1,
            MaxPayloadInMB=10,
            BatchStrategy="SingleRecord",
            TransformInput={
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": predict_s3_prefix,
                    }
                },
                "ContentType": "text/csv",
                "SplitType": "None",
            },
            TransformOutput={
                "S3OutputPath": output_s3_prefix,
                "AssembleWith": "None",
            },
            TransformResources={
                "InstanceType": "ml.c4.8xlarge",
                "InstanceCount": 1,
            },
            Environment={
                # Tell container where train data lives
                "TRAIN_S3_PREFIX": train_s3_prefix,
            },
        )

        return transform_job_name

    def wait_for_transform(self, transform_job_name: str, timeout: int = 1800):
        """
        Polls SageMaker until the transform job completes or fails.
        """
        start = time.time()

        while True:
            resp = self.sm.describe_transform_job(
                TransformJobName=transform_job_name
            )
            status = resp["TransformJobStatus"]

            if status == "Completed":
                return

            if status == "Failed":
                reason = resp.get("FailureReason", "Unknown")
                raise RuntimeError(
                    f"Batch Transform failed: {reason}"
                )

            if time.time() - start > timeout:
                raise TimeoutError("Batch Transform timed out")

            time.sleep(10)
