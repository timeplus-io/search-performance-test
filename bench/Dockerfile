FROM python:3.11-slim
WORKDIR /bench
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV DATA_DIR=/data RESULTS_DIR=/results PYTHONUNBUFFERED=1
ENTRYPOINT ["python"]
