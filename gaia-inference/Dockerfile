FROM python:3.9-slim

WORKDIR /opt/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY inference.py .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "inference.py"]
