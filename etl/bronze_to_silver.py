import argparse
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from langdetect import LangDetectException, detect


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


def _is_valid_http_url(url: str) -> bool:
    return bool(re.match(r"^https?://[^\s/$.?#].[^\s]*$", url.strip(), flags=re.IGNORECASE))


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _clean_text(text: str) -> str:
    if text is None:
        return ""
    t = str(text)
    t = re.sub(r"<[^>]+>", " ", t)
    t = _normalize_whitespace(t)
    return t


def detect_language(text: str) -> Optional[str]:
    sample = _clean_text(text)
    if len(sample) < 20:
        return None
    try:
        return detect(sample)
    except LangDetectException:
        return None


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


def list_objects(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token: Optional[str] = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []) or []:
            k = obj.get("Key")
            if isinstance(k, str):
                keys.append(k)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def get_json(s3, bucket: str, key: str) -> Any:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return json.loads(body)


def put_json(s3, bucket: str, key: str, payload: Any) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )


def validate_article(article: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    titre = _clean_text(article.get("titre", ""))
    url = str(article.get("url", "")).strip()
    date_publication = article.get("date_publication")
    contenu = _clean_text(article.get("contenu", ""))

    if not titre:
        errors.append("titre vide")
    if not date_publication or not str(date_publication).strip():
        errors.append("date manquante")
    if len(contenu) <= 100:
        errors.append("contenu trop court")
    if not _is_valid_http_url(url):
        errors.append("url invalide")

    return errors


def normalize_article(raw: dict[str, Any], default_source: str) -> dict[str, Any]:
    titre = _clean_text(raw.get("titre", ""))
    url = str(raw.get("url", "")).strip()
    date_publication = raw.get("date_publication")
    categorie = _clean_text(raw.get("categorie")) if raw.get("categorie") is not None else None
    contenu = _clean_text(raw.get("contenu", ""))
    source = _clean_text(raw.get("source", "")) or default_source

    langue = detect_language(f"{titre}. {contenu}")

    return {
        "titre": titre,
        "url": url,
        "date_publication": date_publication,
        "categorie": categorie,
        "contenu": contenu,
        "source": source,
        "langue": langue,
        "normalized_at": _now_utc_iso(),
    }


def silver_article_key(source: str, dt: str, url: str) -> str:
    h = _sha1(url)
    return f"silver/source={source}/dt={dt}/articles/{h}.json"


def _dt_from_key(key: str) -> Optional[str]:
    m = re.search(r"/dt=(\d{4}-\d{2}-\d{2})/", key)
    return m.group(1) if m else None


def _extract_articles_from_bronze_payload(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []

    if isinstance(payload.get("articles"), list):
        return [a for a in payload["articles"] if isinstance(a, dict)]

    if payload.get("layer") == "bronze" and isinstance(payload.get("article"), dict):
        return [payload["article"]]

    return []


def bronze_to_silver(
    s3,
    bucket: str,
    bronze_prefix: str,
    default_source: str,
    limit_keys: int,
) -> dict[str, Any]:
    keys = list_objects(s3, bucket=bucket, prefix=bronze_prefix)
    if limit_keys > 0:
        keys = keys[:limit_keys]

    seen_urls: set[str] = set()
    written: list[str] = []
    rejected: list[dict[str, Any]] = []

    for key in keys:
        payload = get_json(s3, bucket=bucket, key=key)
        dt = _dt_from_key(key) or _now_utc().strftime("%Y-%m-%d")

        raws = _extract_articles_from_bronze_payload(payload)
        for raw in raws:
            url = str(raw.get("url", "")).strip()
            if not url:
                rejected.append({"bronze_key": key, "url": url, "errors": ["url manquante"]})
                continue

            if url in seen_urls:
                rejected.append({"bronze_key": key, "url": url, "errors": ["doublon url"]})
                continue

            normalized = normalize_article(raw, default_source=default_source)
            errs = validate_article(normalized)
            if errs:
                rejected.append({"bronze_key": key, "url": url, "errors": errs})
                continue

            source = str(normalized.get("source") or default_source)
            silver_key = silver_article_key(source=source, dt=dt, url=url)

            record = {
                "layer": "silver",
                "bronze_key": key,
                "ingested_at": _now_utc_iso(),
                "article": normalized,
            }

            put_json(s3, bucket=bucket, key=silver_key, payload=record)
            written.append(silver_key)
            seen_urls.add(url)

    run_id = _sha1(_now_utc_iso())[:12]
    manifest_key = f"silver/_manifests/bronze_to_silver_{run_id}.json"
    manifest = {
        "run_id": run_id,
        "started_at": _now_utc_iso(),
        "bucket": bucket,
        "bronze_prefix": bronze_prefix,
        "written": len(written),
        "rejected": len(rejected),
        "written_keys": written,
        "rejected_details": rejected,
    }
    put_json(s3, bucket=bucket, key=manifest_key, payload=manifest)

    return {
        "run_id": run_id,
        "manifest_key": manifest_key,
        "written": len(written),
        "rejected": len(rejected),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="ETL Bronze -> Silver (MinIO)")

    p.add_argument(
        "--bronze-prefix",
        default="bronze/",
        help="Préfixe S3 à lire (ex: bronze/source=hespress/)",
    )
    p.add_argument("--default-source", default="hespress", help="Source par défaut si champ manquant")
    p.add_argument(
        "--limit-keys",
        type=int,
        default=0,
        help="Limiter le nombre d'objets Bronze parcourus (0 = pas de limite)",
    )

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

    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    cfg = S3Config(
        endpoint_url=args.endpoint_url,
        access_key=args.access_key,
        secret_key=args.secret_key,
        region_name=args.region_name,
        bucket=args.bucket,
    )

    s3 = build_s3_client(cfg)
    ensure_bucket(s3, cfg.bucket)

    stats = bronze_to_silver(
        s3=s3,
        bucket=cfg.bucket,
        bronze_prefix=args.bronze_prefix,
        default_source=args.default_source,
        limit_keys=args.limit_keys,
    )

    print(json.dumps({"status": "ok", "stats": stats}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
