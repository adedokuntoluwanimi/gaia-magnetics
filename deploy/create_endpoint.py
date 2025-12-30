import sagemaker
from sagemaker.sklearn.model import SKLearnModel

ROLE_ARN = "arn:aws:iam::425933242610:role/gaia-ec2-s3-role"
BUCKET = "gaia22"
SCRIPT_S3_PATH = "s3://gaia22/ml/inference.tar.gz"

sess = sagemaker.Session()

model = SKLearnModel(
    model_data=None,
    role=ROLE_ARN,
    entry_point="inference.py",
    source_dir=SCRIPT_S3_PATH,
    framework_version="1.2-1",
    py_version="py3",
    sagemaker_session=sess,
)

predictor = model.deploy(
    instance_type="ml.m5.large",
    initial_instance_count=1,
    endpoint_name="gaia-train-on-the-fly",
)

print("Endpoint deployed:", predictor.endpoint_name)
