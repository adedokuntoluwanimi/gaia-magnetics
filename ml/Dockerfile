FROM public.ecr.aws/dlc/xgboost-inference:1.7-1-cpu-py3

WORKDIR /opt/ml/code

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY inference.py .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-c"]
CMD ["from inference import handler; import sys, json; handler(json.load(sys.stdin))"]
