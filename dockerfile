# Dockerfile
FROM apache/airflow:2.8.1-python3.9

ENV PATH="/home/airflow/.local/bin:${PATH}"

COPY requirements.txt .

USER airflow
RUN pip install --no-cache-dir --user -r requirements.txt