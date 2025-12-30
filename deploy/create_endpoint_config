import boto3

REGION = "us-east-1"
MODEL_NAME = "gaia-train-on-the-fly-model"
CONFIG_NAME = "gaia-train-on-the-fly-config"

sm = boto3.client("sagemaker", region_name=REGION)

sm.create_endpoint_config(
    EndpointConfigName=CONFIG_NAME,
    ProductionVariants=[
        {
            "VariantName": "AllTraffic",
            "ModelName": MODEL_NAME,
            "InstanceType": "ml.m5.large",
            "InitialInstanceCount": 1,
        }
    ],
)

print("Endpoint config created:", CONFIG_NAME)
