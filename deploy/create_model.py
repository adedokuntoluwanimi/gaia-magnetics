import boto3
from sagemaker import image_uris

REGION = "us-east-1"
ROLE_ARN = "arn:aws:iam::425933242610:role/gaia-ec2-s3-role"
MODEL_NAME = "gaia-train-on-the-fly-model"



IMAGE_URI = image_uris.retrieve(
    framework="sklearn",
    region=REGION,
    version="1.2-1",
    py_version="py3",
    instance_type="ml.m5.large",
)


SOURCE_DIR_S3 = "s3://gaia22/ml/inference.tar.gz"

sm = boto3.client("sagemaker", region_name=REGION)

response = sm.create_model(
    ModelName=MODEL_NAME,
    ExecutionRoleArn=ROLE_ARN,
    PrimaryContainer={
        "Image": IMAGE_URI,
        "Mode": "SingleModel",
        "Environment": {
            "SAGEMAKER_PROGRAM": "inference.py",
            "SAGEMAKER_SUBMIT_DIRECTORY": SOURCE_DIR_S3,
            "SAGEMAKER_CONTAINER_LOG_LEVEL": "20",
            "SAGEMAKER_REGION": REGION,
        },
    },
)

print("Model created:", MODEL_NAME)
