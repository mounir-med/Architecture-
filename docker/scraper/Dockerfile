FROM python:3.11-slim

WORKDIR /app

COPY scrapers/requirements.txt /tmp/req_scrapers.txt
COPY ingestion/requirements.txt /tmp/req_ingestion.txt
COPY datalake/requirements.txt /tmp/req_datalake.txt
COPY etl/requirements.txt /tmp/req_etl.txt
COPY etl/requirements_gold.txt /tmp/req_gold.txt
COPY warehouse/requirements.txt /tmp/req_warehouse.txt

RUN pip install --no-cache-dir -r /tmp/req_scrapers.txt \
 && pip install --no-cache-dir -r /tmp/req_ingestion.txt \
 && pip install --no-cache-dir -r /tmp/req_datalake.txt \
 && pip install --no-cache-dir -r /tmp/req_etl.txt \
 && pip install --no-cache-dir -r /tmp/req_gold.txt \
 && pip install --no-cache-dir -r /tmp/req_warehouse.txt

COPY scrapers/ /app/scrapers/
COPY ingestion/ /app/ingestion/
COPY datalake/ /app/datalake/
COPY etl/ /app/etl/
COPY warehouse/ /app/warehouse/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-c", "print('ready')"]
