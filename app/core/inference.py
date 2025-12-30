import json
import csv
from pathlib import Path
from typing import List, Dict

import boto3


# ============================================================
# Configuration
# ============================================================

SAGEMAKER_ENDPOINT_NAME = "gaia-magnetics-endpoint"
AWS_REGION = "us-east-1"


# ============================================================
# SageMaker client
# ============================================================

_runtime = boto3.client(
    "sagemaker-runtime",
    region_name=AWS_REGION,
)


# ============================================================
# Inference runner
# ============================================================

def run_sagemaker_inference(
    train_csv: Path,
    predict_csv: Path,
) -> List[Dict]:
    """
    Calls a SageMaker endpoint to infer magnetic values.

    Expected behavior:
    - Model learns magnetic_value vs distance_along from train
    - Model predicts magnetic_value for predict distances
    - Returned rows must include distance_along and magnetic_value

    Returns:
    - List of predicted rows
    """

    # ----------------------------
    # Load train data
    # ----------------------------
    train_rows = _read_csv(train_csv)
    predict_rows = _read_csv(predict_csv)

    if not predict_rows:
        return []

    # ----------------------------
    # Prepare payload
    # ----------------------------
    payload = {
        "train": [
            {
                "distance_along": float(r["distance_along"]),
                "value": float(r["magnetic_value"]),
            }
            for r in train_rows
        ],
        "predict": [
            {
                "distance_along": float(r["distance_along"]),
            }
            for r in predict_rows
        ],
    }

    # ----------------------------
    # Invoke endpoint
    # ----------------------------
    response = _runtime.invoke_endpoint(
        EndpointName=SAGEMAKER_ENDPOINT_NAME,
        ContentType="application/json",
        Body=json.dumps(payload),
    )

    # ----------------------------
    # Parse response
    # ----------------------------
    body = response["Body"].read().decode("utf-8")
    result = json.loads(body)

    if "predictions" not in result:
        raise RuntimeError("Invalid response from SageMaker endpoint")

    predictions = []
    for item in result["predictions"]:
        predictions.append({
            "distance_along": float(item["distance_along"]),
            "magnetic_value": float(item["magnetic_value"]),
        })

    return predictions


# ============================================================
# Internal helpers
# ============================================================

def _read_csv(path: Path) -> List[Dict]:
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)
