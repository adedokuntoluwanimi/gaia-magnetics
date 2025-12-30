# app/core/inference.py

import json
from typing import List
import boto3


class SageMakerInference:
    """
    SageMaker inference wrapper.

    Contract (LOCKED):
    - Endpoint receives S3 URIs for train.csv and predict.csv
    - Endpoint is responsible for:
        * loading train.csv
        * fitting model
        * predicting predict.csv
    - Endpoint returns one prediction per predict row
    - Order is preserved
    """

    def __init__(self, endpoint_name: str, region: str | None = None):
        self.endpoint_name = endpoint_name
        self.runtime = boto3.client(
            "sagemaker-runtime",
            region_name=region,
        )

    def predict(
        self,
        *,
        train_s3_uri: str,
        predict_s3_uri: str,
    ) -> List[float]:
        """
        Invoke SageMaker endpoint using train-on-the-fly inference.
        """

        payload = {
            "train_uri": train_s3_uri,
            "predict_uri": predict_s3_uri,
        }

        response = self.runtime.invoke_endpoint(
            EndpointName=self.endpoint_name,
            ContentType="application/json",
            Body=json.dumps(payload),
        )

        body = response["Body"].read().decode("utf-8")

        # ---------------------------------------------
        # Parse response
        # ---------------------------------------------

        # Preferred: JSON array
        try:
            data = json.loads(body)
            if isinstance(data, list):
                return [float(v) for v in data]
        except json.JSONDecodeError:
            pass

        # Fallback: newline / CSV style
        predictions: List[float] = []

        for line in body.splitlines():
            line = line.strip()
            if not line:
                continue
            predictions.append(float(line))

        if not predictions:
            raise ValueError("No predictions returned from SageMaker endpoint")

        return predictions
