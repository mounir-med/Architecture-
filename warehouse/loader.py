import argparse
import json
import os
from dataclasses import dataclass
from typing import Any, Optional

import boto3
import psycopg2
from botocore.client import Config
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class S3Config:
    endpoint_url: str
    access_key: str
    secret_key: str
    region_name: str
    bucket: str


@dataclass(frozen=True)
class PgConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def build_s3_client(cfg: S3Config):
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        region_name=cfg.region_name,
        config=Config(signature_version="s3v4"),
    )


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def get_json(s3, bucket: str, key: str) -> Any:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return json.loads(body)


def connect_pg(cfg: PgConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )


def upsert_articles_par_jour(cur, rows: list[dict[str, Any]]) -> int:
    q = (
        "INSERT INTO articles_par_jour(date, nb_articles, updated_at) "
        "VALUES (%s, %s, NOW()) "
        "ON CONFLICT (date) DO UPDATE SET nb_articles = EXCLUDED.nb_articles, updated_at = NOW()"
    )
    n = 0
    for r in rows:
        cur.execute(q, (r.get("date"), int(r.get("nb_articles", 0))))
        n += 1
    return n


def upsert_articles_par_source(cur, rows: list[dict[str, Any]]) -> int:
    q = (
        "INSERT INTO articles_par_source(source, nb_articles, updated_at) "
        "VALUES (%s, %s, NOW()) "
        "ON CONFLICT (source) DO UPDATE SET nb_articles = EXCLUDED.nb_articles, updated_at = NOW()"
    )
    n = 0
    for r in rows:
        cur.execute(q, (r.get("source"), int(r.get("nb_articles", 0))))
        n += 1
    return n


def upsert_mots_cles(cur, rows: list[dict[str, Any]]) -> int:
    q = (
        "INSERT INTO mots_cles(mot_cle, count, updated_at) "
        "VALUES (%s, %s, NOW()) "
        "ON CONFLICT (mot_cle) DO UPDATE SET count = EXCLUDED.count, updated_at = NOW()"
    )
    n = 0
    for r in rows:
        cur.execute(q, (r.get("mot_cle"), int(r.get("count", 0))))
        n += 1
    return n


def upsert_top_sujets(cur, rows: list[dict[str, Any]]) -> int:
    q = (
        "INSERT INTO top_sujets(sujet, count, updated_at) "
        "VALUES (%s, %s, NOW()) "
        "ON CONFLICT (sujet) DO UPDATE SET count = EXCLUDED.count, updated_at = NOW()"
    )
    n = 0
    for r in rows:
        cur.execute(q, (r.get("sujet"), int(r.get("count", 0))))
        n += 1
    return n


def load_gold_dataset(s3, bucket: str, key: str) -> list[dict[str, Any]]:
    payload = get_json(s3, bucket=bucket, key=key)
    rows = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise ValueError(f"Gold dataset invalide: {key} (clé 'rows' manquante)")
    return [r for r in rows if isinstance(r, dict)]


def run_load(
    s3,
    bucket: str,
    pg_cfg: PgConfig,
    gold_prefix: str,
) -> dict[str, Any]:
    keys = {
        "articles_par_jour": f"{gold_prefix}/articles_par_jour.json",
        "articles_par_source": f"{gold_prefix}/articles_par_source.json",
        "mots_cles": f"{gold_prefix}/mots_cles.json",
        "top_sujets": f"{gold_prefix}/top_sujets.json",
    }

    datasets = {name: load_gold_dataset(s3, bucket=bucket, key=k) for name, k in keys.items()}

    with connect_pg(pg_cfg) as conn:
        with conn.cursor() as cur:
            n1 = upsert_articles_par_jour(cur, datasets["articles_par_jour"])
            n2 = upsert_articles_par_source(cur, datasets["articles_par_source"])
            n3 = upsert_mots_cles(cur, datasets["mots_cles"])
            n4 = upsert_top_sujets(cur, datasets["top_sujets"])

        conn.commit()

    return {"loaded": {"articles_par_jour": n1, "articles_par_source": n2, "mots_cles": n3, "top_sujets": n4}}


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Loader Gold -> PostgreSQL")

    p.add_argument(
        "--gold-prefix",
        required=True,
        help="Préfixe Gold complet, ex: gold/dt=2026-05-04/run_id=xxxxxxxxxxxx",
    )

    p.add_argument(
        "--endpoint-url",
        default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        help="Endpoint MinIO (S3)",
    )
    p.add_argument(
        "--access-key",
        default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        help="Access key MinIO",
    )
    p.add_argument(
        "--secret-key",
        default=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        help="Secret key MinIO",
    )
    p.add_argument(
        "--region-name",
        default=os.getenv("MINIO_REGION", "us-east-1"),
        help="Region",
    )
    p.add_argument(
        "--bucket",
        default=os.getenv("DATALAKE_BUCKET", "media-datalake"),
        help="Bucket",
    )

    p.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"))
    p.add_argument("--pg-port", type=int, default=int(os.getenv("PGPORT", "5432")))
    p.add_argument("--pg-db", default=os.getenv("PGDATABASE", "media"))
    p.add_argument("--pg-user", default=os.getenv("PGUSER", "media"))
    p.add_argument("--pg-password", default=os.getenv("PGPASSWORD", "media"))

    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    s3_cfg = S3Config(
        endpoint_url=args.endpoint_url,
        access_key=args.access_key,
        secret_key=args.secret_key,
        region_name=args.region_name,
        bucket=args.bucket,
    )
    pg_cfg = PgConfig(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_password,
    )

    s3 = build_s3_client(s3_cfg)
    ensure_bucket(s3, s3_cfg.bucket)

    stats = run_load(s3=s3, bucket=s3_cfg.bucket, pg_cfg=pg_cfg, gold_prefix=args.gold_prefix)

    print(json.dumps({"status": "ok", "stats": stats}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
