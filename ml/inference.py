def predict_fn(payload, model):
    import io
    import boto3
    import pandas as pd
    import numpy as np
    from sklearn.linear_model import LinearRegression

    train_uri = payload["train_uri"]
    predict_uri = payload["predict_uri"]

    # --- read CSVs from S3 ---
    def read_csv_from_s3(s3_uri):
        if not s3_uri.startswith("s3://"):
            raise ValueError("Invalid S3 URI")

        _, _, bucket_key = s3_uri.partition("s3://")
        bucket, _, key = bucket_key.partition("/")

        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")

        return pd.read_csv(io.StringIO(body), header=None)

    train_df = read_csv_from_s3(train_uri)
    train_df.columns = ["x", "y", "d_along", "tmi"]

    predict_df = read_csv_from_s3(predict_uri)
    predict_df.columns = ["x", "y", "d_along"]

    X_train = train_df[["d_along"]].values
    y_train = train_df["tmi"].values
    X_predict = predict_df[["d_along"]].values

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_predict)

    # --- boundary clamping ---
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
