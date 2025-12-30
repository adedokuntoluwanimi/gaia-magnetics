import boto3

REGION = "us-east-1"
ENDPOINT_NAME = "gaia-train-on-the-fly"
CONFIG_NAME = "gaia-train-on-the-fly-config"

sm = boto3.client("sagemaker", region_name=REGION)

sm.create_endpoint(
    EndpointName=ENDPOINT_NAME,
    EndpointConfigName=CONFIG_NAME,
)

print("Endpoint creation started:", ENDPOINT_NAME)
