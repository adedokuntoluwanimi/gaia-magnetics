import json
import tempfile
import boto3
import pandas as pd
import xgboost as xgb
from io import StringIO


s3 = boto3.client("s3")


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])


def write_csv_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


def run_inference(
    bucket: str,
    train_key: str,
    predict_key: str,
    output_key: str,
) -> None:
    # -----------------------------
    # Load data
    # -----------------------------
    train_df = read_csv_from_s3(bucket, train_key)
    predict_df = read_csv_from_s3(bucket, predict_key)

    if train_df.empty:
        raise ValueError("Training CSV is empty")

    if predict_df.empty:
        raise ValueError("Prediction CSV is empty")

    # -----------------------------
    # Prepare features
    # -----------------------------
    X_train = train_df[["d_along"]]
    y_train = train_df["tmi"]

    X_pred = predict_df[["d_along"]]

    # -----------------------------
    # Train model
    # -----------------------------
    model = xgb.XGBRegressor(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="reg:squarederror",
        random_state=42,
    )

    model.fit(X_train, y_train)

    # -----------------------------
    # Predict
    # -----------------------------
    preds = model.predict(X_pred)

    # -----------------------------
    # Save predictions
    # -----------------------------
    out_df = pd.DataFrame({
        "d_along": predict_df["d_along"],
        "predicted_tmi": preds,
    })

    write_csv_to_s3(out_df, bucket, output_key)


# ======================================================
# SageMaker entrypoint
# ======================================================
def handler(event, context=None):
    """
    Expected event:
    {
      "bucket": "...",
      "train_key": "...",
      "predict_key": "...",
      "output_key": "..."
    }
    """

    required = ["bucket", "train_key", "predict_key", "output_key"]
    for k in required:
        if k not in event:
            raise ValueError(f"Missing required key: {k}")

    run_inference(
        bucket=event["bucket"],
        train_key=event["train_key"],
        predict_key=event["predict_key"],
        output_key=event["output_key"],
    )

    return {
        "status": "ok",
        "output_key": event["output_key"],
    }
