FROM python:3.9-slim

# -----------------------------
# System deps
# -----------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Set working dir
# -----------------------------
WORKDIR /opt/ml/code

# -----------------------------
# Install Python deps
# -----------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------
# Copy inference code
# -----------------------------
COPY inference.py .

# -----------------------------
# SageMaker entrypoint
# -----------------------------
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-c"]
CMD ["from inference import handler; import sys, json; handler(json.load(sys.stdin))"]
