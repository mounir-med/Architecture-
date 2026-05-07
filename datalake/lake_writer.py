import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class S3Config:
    endpoint_url: str
    access_key: str
    secret_key: str
    region_name: str
    bucket: str


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_utc_iso() -> str:
    return _now_utc().isoformat()


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def load_scraper_output(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError("JSON invalide: attendu un objet")
    return payload


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
        return
    except ClientError:
        pass

    s3.create_bucket(Bucket=bucket)


def put_json(s3, bucket: str, key: str, payload: Any) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )


def bronze_key(source: str, event_time: datetime, batch_id: str) -> str:
    dt = event_time.strftime("%Y-%m-%d")
    ts = event_time.strftime("%H%M%S")
    return f"bronze/source={source}/dt={dt}/{ts}_{batch_id}.json"


def bronze_article_key(source: str, event_time: datetime, url: str) -> str:
    dt = event_time.strftime("%Y-%m-%d")
    h = _sha1(url)
    return f"bronze/source={source}/dt={dt}/articles/{h}.json"


def write_bronze_batch(
    s3,
    bucket: str,
    payload: dict[str, Any],
    source: str,
    split_per_article: bool,
) -> dict[str, Any]:
    event_time = _now_utc()
    batch_id = _sha1(f"{source}|{_now_utc_iso()}")[:12]

    articles = payload.get("articles")
    if not isinstance(articles, list):
        raise ValueError("Format JSON invalide: clé 'articles' manquante ou non-liste")

    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}

    written_keys: list[str] = []

    if split_per_article:
        for a in articles:
            if not isinstance(a, dict):
                continue
            url = str(a.get("url", "")).strip()
            if not url:
                continue
            key = bronze_article_key(source=source, event_time=event_time, url=url)
            record = {
                "ingested_at": _now_utc_iso(),
                "source": source,
                "layer": "bronze",
                "article": a,
            }
            put_json(s3, bucket=bucket, key=key, payload=record)
            written_keys.append(key)
    else:
        key = bronze_key(source=source, event_time=event_time, batch_id=batch_id)
        record = {
            "ingested_at": _now_utc_iso(),
            "source": source,
            "layer": "bronze",
            "meta": meta,
            "articles": articles,
        }
        put_json(s3, bucket=bucket, key=key, payload=record)
        written_keys.append(key)

    return {
        "bucket": bucket,
        "written": len(written_keys),
        "keys": written_keys,
        "split_per_article": split_per_article,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="MinIO writer - Bronze layer (S3)")
    p.add_argument("--input-json", required=True, help="Fichier JSON local (sortie scraper)")
    p.add_argument("--source", default="hespress", help="Nom de la source")

    p.add_argument(
        "--endpoint-url",
        default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        help="Endpoint MinIO (S3), ex: http://localhost:9000",
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
        help="Region (S3 compat)",
    )
    p.add_argument(
        "--bucket",
        default=os.getenv("DATALAKE_BUCKET", "media-datalake"),
        help="Bucket du Data Lake",
    )
    p.add_argument(
        "--split-per-article",
        action="store_true",
        help="Si présent: écrit 1 objet par article (sinon 1 batch)",
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    payload = load_scraper_output(args.input_json)

    cfg = S3Config(
        endpoint_url=args.endpoint_url,
        access_key=args.access_key,
        secret_key=args.secret_key,
        region_name=args.region_name,
        bucket=args.bucket,
    )

    s3 = build_s3_client(cfg)
    ensure_bucket(s3, cfg.bucket)

    stats = write_bronze_batch(
        s3=s3,
        bucket=cfg.bucket,
        payload=payload,
        source=args.source,
        split_per_article=args.split_per_article,
    )

    print(json.dumps({"status": "ok", "stats": stats}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
