
FROM data_engineering_lab-airflow-webserver:latest

WORKDIR /app

COPY ./load_olist_to_postgres.py /app/load_olist_to_postgres.py
COPY ./olist_data /app/olist_data