from __future__ import annotations

# 2026-05-09: DAG updated to run 7 scrapers + 7 bronze writes in parallel, then a single bronze_to_silver over bronze/ after all bronze writes complete.

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "media",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="media_pipeline_hourly",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["media", "bigdata"],
) as dag:
    def compute_gold_prefix(**context):
        import hashlib
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        dt = now.strftime("%Y-%m-%d")
        run_id = hashlib.sha1(now.isoformat().encode("utf-8")).hexdigest()[:12]
        context["ti"].xcom_push(key="gold_prefix", value=f"gold/dt={dt}/run_id={run_id}")

    gold_prefix = PythonOperator(
        task_id="compute_gold_prefix",
        python_callable=compute_gold_prefix,
    )

    sources = [
        "hespress",
        "akhbarona",
        "goud",
        "barlamane",
        "aljazeera",
        "bbc_arabic",
    ]

    scrape_tasks: dict[str, BashOperator] = {}
    bronze_write_tasks: dict[str, BashOperator] = {}

    for source in sources:
        scrape_tasks[source] = BashOperator(
            task_id=f"scrape_{source}",
            bash_command=(
                f"python /app/scrapers/{source}_scraper.py "
                "--max-articles 20 "
                f"--output /tmp/{source}_articles.json"
            ),
        )

        bronze_write_tasks[source] = BashOperator(
            task_id=f"bronze_write_{source}",
            bash_command=(
                "python /app/datalake/lake_writer.py "
                f"--input-json /tmp/{source}_articles.json "
                f"--source {source} "
                "--split-per-article"
            ),
            env={
                "MINIO_ENDPOINT": "http://minio:9000",
                "MINIO_ACCESS_KEY": "minioadmin",
                "MINIO_SECRET_KEY": "minioadmin",
                "DATALAKE_BUCKET": "media-datalake",
            },
        )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "python /app/etl/bronze_to_silver.py "
            "--bronze-prefix bronze/ "
            "--default-source hespress"
        ),
        env={
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "DATALAKE_BUCKET": "media-datalake",
        },
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "python /app/etl/silver_to_gold.py "
            "--silver-prefix silver/ "
            "--top-n 50 "
            "--gold-prefix \"{{ ti.xcom_pull(task_ids='compute_gold_prefix', key='gold_prefix') }}\""
        ),
        env={
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "DATALAKE_BUCKET": "media-datalake",
        },
    )

    load_gold_to_postgres = BashOperator(
        task_id="load_gold_to_postgres",
        bash_command=(
            "python /app/warehouse/loader.py "
            "--gold-prefix \"{{ ti.xcom_pull(task_ids='compute_gold_prefix', key='gold_prefix') }}\" "
            "--pg-host postgres --pg-port 5432 --pg-db media --pg-user media --pg-password media"
        ),
        env={
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "DATALAKE_BUCKET": "media-datalake",
        },
    )

    for source in sources:
        gold_prefix >> scrape_tasks[source] >> bronze_write_tasks[source]
        bronze_write_tasks[source] >> bronze_to_silver

    bronze_to_silver >> silver_to_gold >> load_gold_to_postgres
