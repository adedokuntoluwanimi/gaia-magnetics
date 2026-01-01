import os
import pandas as pd
from sklearn.linear_model import LinearRegression


# --------------------------------------------------
# SageMaker Batch Transform fixed paths
# --------------------------------------------------
BASE_INPUT = "/opt/ml/input/data"

TRAIN_PATH = os.path.join(BASE_INPUT, "train", "train.csv")
PREDICT_PATH = os.path.join(BASE_INPUT, "predict", "predict.csv")

OUTPUT_DIR = "/opt/ml/output"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "predictions.csv")


# --------------------------------------------------
# Entry point
# --------------------------------------------------
def main():
    # ----------------------------
    # Load inputs
    # ----------------------------
    if not os.path.exists(TRAIN_PATH):
        raise RuntimeError(f"Missing train.csv at {TRAIN_PATH}")

    if not os.path.exists(PREDICT_PATH):
        raise RuntimeError(f"Missing predict.csv at {PREDICT_PATH}")

    train_df = pd.read_csv(TRAIN_PATH)
    predict_df = pd.read_csv(PREDICT_PATH)

    if train_df.empty:
        raise RuntimeError("train.csv is empty")

    if predict_df.empty:
        raise RuntimeError("predict.csv is empty")

    # ----------------------------
    # Validate required columns
    # ----------------------------
    if "distance_along" not in train_df.columns:
        raise RuntimeError("train.csv missing distance_along column")

    if "distance_along" not in predict_df.columns:
        raise RuntimeError("predict.csv missing distance_along column")

    # Infer target column from train.csv
    value_cols = [
        c for c in train_df.columns
        if c not in ("distance_along", "is_measured")
    ]

    if len(value_cols) != 1:
        raise RuntimeError(
            f"Expected exactly one value column in train.csv, found {value_cols}"
        )

    value_col = value_cols[0]

    # ----------------------------
    # Fit job-local model
    # ----------------------------
    X_train = train_df[["distance_along"]]
    y_train = train_df[value_col]

    model = LinearRegression()
    model.fit(X_train, y_train)

    # ----------------------------
    # Predict on unmeasured points
    # ----------------------------
    X_pred = predict_df[["distance_along"]]
    predictions = model.predict(X_pred)

    output_df = pd.DataFrame(
        {
            "distance_along": predict_df["distance_along"],
            "predicted_value": predictions,
        }
    )

    # ----------------------------
    # Write predictions
    # ----------------------------
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_df.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    main()
