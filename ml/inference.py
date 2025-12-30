# ml/inference.py

import json
import io
import boto3
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def read_csv_from_s3(s3_uri: str) -> pd.DataFrame:
    """
    Read a headerless CSV from S3 into a DataFrame.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI")

    _, _, bucket_key = s3_uri.partition("s3://")
    bucket, _, key = bucket_key.partition("/")

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")

    return pd.read_csv(io.StringIO(body), header=None)


# -------------------------------------------------
# SageMaker required functions
# -------------------------------------------------

def model_fn(model_dir):
    """
    Required by SageMaker.
    We do not load a persisted model.
    """
    return None


def input_fn(request_body, content_type):
    """
    Parse incoming request.
    """
    if content_type != "application/json":
        raise ValueError("Unsupported content type")

    payload = json.loads(request_body)

    if "train_uri" not in payload or "predict_uri" not in payload:
        raise ValueError("Payload must contain train_uri and predict_uri")

    return payload


def predict_fn(payload, model):
    """
    Core logic:
    - Load train.csv and predict.csv
    - Train model
    - Predict
    - Apply boundary clamping
    """

    train_uri = payload["train_uri"]
    predict_uri = payload["predict_uri"]

    # ---------------------------------------------
    # Load data
    # ---------------------------------------------

    # train.csv: x, y, d_along, tmi
    train_df = read_csv_from_s3(train_uri)
    train_df.columns = ["x", "y", "d_along", "tmi"]

    # predict.csv: x, y, d_along
    predict_df = read_csv_from_s3(predict_uri)
    predict_df.columns = ["x", "y", "d_along"]

    # ---------------------------------------------
    # Prepare features
    # ---------------------------------------------

    X_train = train_df[["d_along"]].values
    y_train = train_df["tmi"].values

    X_predict = predict_df[["d_along"]].values

    # ---------------------------------------------
    # Train model (deterministic)
    # ---------------------------------------------

    model = LinearRegression()
    model.fit(X_train, y_train)

    # ---------------------------------------------
    # Predict
    # ---------------------------------------------

    y_pred = model.predict(X_predict)

    # ---------------------------------------------
    # Boundary clamping
    # ---------------------------------------------

    min_d = X_train.min()
    max_d = X_train.max()

    start_value = y_train[X_train.argmin()]
    end_value = y_train[X_train.argmax()]

    for i, d in enumerate(X_predict.flatten()):
        if d < min_d:
            y_pred[i] = start_value
        elif d > max_d:
            y_pred[i] = end_value

    return y_pred.tolist()


def output_fn(predictions, accept):
    """
    Return predictions to backend.
    """
    if accept == "application/json":
        return json.dumps(predictions), accept

    # Fallback: plain text
    body = "\n".join(str(v) for v in predictions)
    return body, "text/plain"
