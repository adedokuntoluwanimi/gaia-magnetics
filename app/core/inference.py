import os
import pandas as pd
from sklearn.linear_model import LinearRegression


# --------------------------------------------------
# SageMaker Batch Transform paths (fixed)
# --------------------------------------------------
BASE_INPUT = "/opt/ml/input/data"
TRAIN_PATH = os.path.join(BASE_INPUT, "train", "train.csv")
PREDICT_PATH = os.path.join(BASE_INPUT, "predict", "predict.csv")

OUTPUT_DIR = "/opt/ml/output"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "predictions.csv")


def main():
    # ----------------------------
    # Load input CSVs
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

    if "magnetic_value" not in train_df.columns:
        raise RuntimeError("train.csv missing magnetic_value column")

    if "distance_along" not in predict_df.columns:
        raise RuntimeError("predict.csv missing distance_along column")

    # ----------------------------
    # Fit job-local model
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
    # Write predictions
    # ----------------------------
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    predict_df.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    main()
