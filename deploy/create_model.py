import boto3

REGION = "us-east-1"
ROLE_ARN = "arn:aws:iam::425933242610:role/gaia-ec2-s3-role"
MODEL_NAME = "gaia-train-on-the-fly-model"

# ✅ Correct, SageMaker-approved sklearn image for us-east-1
IMAGE_URI = (
    "683313688378.dkr.ecr.us-east-1.amazonaws.com/"
    "sagemaker-scikit-learn:1.2-1-cpu-py3"
)

SOURCE_DIR_S3 = "s3://gaia22/ml/inference.tar.gz"

sm = boto3.client("sagemaker", region_name=REGION)

sm.create_model(
    ModelName=MODEL_NAME,
    ExecutionRoleArn=ROLE_ARN,
    PrimaryContainer={
        "Image": IMAGE_URI,
        "Mode": "SingleModel",
        "Environment": {
            "SAGEMAKER_PROGRAM": "inference.py",
            "SAGEMAKER_SUBMIT_DIRECTORY": SOURCE_DIR_S3,
            "SAGEMAKER_REGION": REGION,
        },
    },
)

print("Model created:", MODEL_NAME)
