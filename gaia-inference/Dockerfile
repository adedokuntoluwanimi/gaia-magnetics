FROM python:3.9-slim

# --------------------------------------------------
# Working directory expected by SageMaker
# --------------------------------------------------
WORKDIR /opt/app

# --------------------------------------------------
# Install dependencies
# --------------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --------------------------------------------------
# Copy inference code
# --------------------------------------------------
COPY inference.py .

# --------------------------------------------------
# Disable buffering for clean logs
# --------------------------------------------------
ENV PYTHONUNBUFFERED=1

# --------------------------------------------------
# Entry point for Batch Transform
# --------------------------------------------------
ENTRYPOINT ["python", "inference.py"]
