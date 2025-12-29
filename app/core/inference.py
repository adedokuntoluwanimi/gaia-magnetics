import boto3



class SageMakerInference:
    def __init__(self, endpoint_name: str, region: str = "us-east-1"):
        self.endpoint_name = endpoint_name
        self.runtime = boto3.client("sagemaker-runtime", region_name=region)
        self.s3 = boto3.client("s3", region_name=region)

    def run_from_s3(
        self,
        bucket: str,
        predict_key: str,
        output_key: str,
    ):
        """
        - Reads predict.csv from S3
        - Sends to SageMaker endpoint as text/csv
        - Writes predictions.csv back to S3
        """

        # 1. Download predict.csv
        obj = self.s3.get_object(Bucket=bucket, Key=predict_key)
        payload = obj["Body"].read()

        if not payload:
            raise RuntimeError("predict.csv is empty")

        # 2. Invoke endpoint
        response = self.runtime.invoke_endpoint(
            EndpointName=self.endpoint_name,
            ContentType="text/csv",
            Body=payload,
        )

        predictions = response["Body"].read()

        if not predictions:
            raise RuntimeError("endpoint returned empty predictions")

        # 3. Upload predictions.csv
        self.s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=predictions,
            ContentType="text/csv",
        )

        return {
            "status": "PREDICTED",
            "endpoint": self.endpoint_name,
            "rows": predictions.decode("utf-8").count("\n") + 1,
        }
