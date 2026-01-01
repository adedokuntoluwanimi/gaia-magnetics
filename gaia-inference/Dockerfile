FROM python:3.9-slim

# --------------------------------------------------
# System setup
# --------------------------------------------------
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# --------------------------------------------------
# Install Python dependencies
# --------------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --------------------------------------------------
# Copy inference code
# --------------------------------------------------
COPY inference.py .

# --------------------------------------------------
# SageMaker Batch Transform entrypoint
# --------------------------------------------------
ENTRYPOINT ["python", "inference.py"]
