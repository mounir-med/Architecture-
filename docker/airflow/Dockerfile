FROM apache/airflow:2.9.3

USER root

COPY scrapers/requirements.txt /tmp/req_scrapers.txt
COPY ingestion/requirements.txt /tmp/req_ingestion.txt
COPY datalake/requirements.txt /tmp/req_datalake.txt
COPY etl/requirements.txt /tmp/req_etl.txt
COPY etl/requirements_gold.txt /tmp/req_gold.txt
COPY warehouse/requirements.txt /tmp/req_warehouse.txt

USER airflow

RUN pip install --no-cache-dir \
    -r /tmp/req_scrapers.txt \
    -r /tmp/req_ingestion.txt \
    -r /tmp/req_datalake.txt \
    -r /tmp/req_etl.txt \
    -r /tmp/req_gold.txt \
    -r /tmp/req_warehouse.txt