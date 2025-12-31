import json
import os
import boto3
import pandas as pd
from sklearn.linear_model import LinearRegression


def main():
    # SageMaker async provides the input S3 URI via env var
    input_location = os.environ.get("SM_INPUT_LOCATION")
    if not input_location:
        raise RuntimeError("SM_INPUT_LOCATION not set")

    s3 = boto3.client("s3")

    # ----------------------------
    # Load request.json
    # ----------------------------
    bucket, key = parse_s3_uri(input_location)
    request_obj = s3.get_object(Bucket=bucket, Key=key)
    payload = json.loads(request_obj["Body"].read())

    train_s3 = payload["train_s3"]
    predict_s3 = payload["predict_s3"]
    output_s3 = payload["output_s3"]

    # ----------------------------
    # Load CSVs
    # ----------------------------
    train_df = read_csv_from_s3(s3, train_s3)
    predict_df = read_csv_from_s3(s3, predict_s3)

    # ----------------------------
    # Train local model
    # ----------------------------
    X_train = train_df[["distance_along"]]
    y_train = train_df["magnetic_value"]

    model = LinearRegression()
    model.fit(X_train, y_train)

    # ----------------------------
    # Predict
    # ----------------------------
    X_pred = predict_df[["distance_along"]]
    predict_df["magnetic_value"] = model.predict(X_pred)

    # ----------------------------
    # Write predictions.csv
    # ----------------------------
    write_csv_to_s3(s3, predict_df, output_s3)


def parse_s3_uri(uri: str):
    assert uri.startswith("s3://")
    parts = uri.replace("s3://", "").split("/", 1)
    return parts[0], parts[1]


def read_csv_from_s3(s3, uri: str) -> pd.DataFrame:
    bucket, key = parse_s3_uri(uri)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])


def write_csv_to_s3(s3, df: pd.DataFrame, uri: str):
    bucket, key = parse_s3_uri(uri)
    csv_bytes = df.to_csv(index=False).encode()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_bytes,
        ContentType="text/csv",
    )


if __name__ == "__main__":
    main()
