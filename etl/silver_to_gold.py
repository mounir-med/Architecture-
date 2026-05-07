import argparse
import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dateutil import parser as dtparser


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


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _extract_article_from_silver_payload(payload: Any) -> Optional[dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    a = payload.get("article")
    return a if isinstance(a, dict) else None


def _dt_from_key(key: str) -> Optional[str]:
    m = re.search(r"/dt=(\d{4}-\d{2}-\d{2})/", key)
    return m.group(1) if m else None


def _parse_publication_day(date_value: Any) -> Optional[str]:
    if date_value is None:
        return None
    raw = str(date_value).strip()
    if not raw:
        return None
    try:
        dt = dtparser.parse(raw)
        return dt.date().isoformat()
    except Exception:
        return None


_STOPWORDS_FR = {
    "avec",
    "avoir",
    "cette",
    "comme",
    "dans",
    "des",
    "donc",
    "elle",
    "elles",
    "est",
    "et",
    "être",
    "fois",
    "faire",
    "mais",
    "même",
    "nous",
    "par",
    "pas",
    "plus",
    "pour",
    "que",
    "qui",
    "sur",
    "tout",
    "une",
    "vos",
    "vous",
}

_STOPWORDS_EN = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "from",
    "are",
    "was",
    "were",
    "have",
    "has",
    "had",
    "not",
    "but",
    "you",
    "your",
    "their",
    "they",
    "his",
    "her",
    "its",
    "into",
    "over",
    "more",
}

# Minimal Arabic stopwords set (very small on purpose)
_STOPWORDS_AR = {
    "من",
    "في",
    "على",
    "إلى",
    "عن",
    "أن",
    "إن",
    "كان",
    "كما",
    "هذا",
    "هذه",
    "هناك",
}


def _tokenize(text: str) -> list[str]:
    t = _normalize_whitespace(text.lower())
    # Keep letters (latin + arabic) and digits
    t = re.sub(r"[^0-9a-z\u0600-\u06FF]+", " ", t, flags=re.IGNORECASE)
    tokens = [x for x in t.split(" ") if x]
    return tokens


def _is_candidate_token(tok: str) -> bool:
    if len(tok) < 4:
        return False
    if tok.isdigit():
        return False
    return True


def _is_stopword(tok: str, lang: Optional[str]) -> bool:
    if not lang:
        return tok in _STOPWORDS_FR or tok in _STOPWORDS_EN or tok in _STOPWORDS_AR
    if lang.startswith("fr"):
        return tok in _STOPWORDS_FR
    if lang.startswith("en"):
        return tok in _STOPWORDS_EN
    if lang.startswith("ar"):
        return tok in _STOPWORDS_AR
    return tok in _STOPWORDS_FR or tok in _STOPWORDS_EN or tok in _STOPWORDS_AR


def aggregate_gold(
    s3,
    bucket: str,
    silver_prefix: str,
    limit_keys: int,
    top_n_keywords: int,
    gold_prefix: Optional[str],
) -> dict[str, Any]:
    keys = list_objects(s3, bucket=bucket, prefix=silver_prefix)
    if limit_keys > 0:
        keys = keys[:limit_keys]

    # Aggregations
    articles_par_jour: dict[str, int] = defaultdict(int)
    articles_par_source: dict[str, int] = defaultdict(int)
    keyword_counter: Counter[str] = Counter()
    sujets_counter: Counter[str] = Counter()  # simple proxy: bigrams

    processed = 0
    rejected = 0

    for key in keys:
        payload = get_json(s3, bucket=bucket, key=key)
        article = _extract_article_from_silver_payload(payload)
        if not article:
            rejected += 1
            continue

        source = str(article.get("source", "")).strip() or "unknown"
        lang = article.get("langue")
        titre = str(article.get("titre", ""))
        contenu = str(article.get("contenu", ""))

        pub_day = _parse_publication_day(article.get("date_publication"))
        if not pub_day:
            pub_day = _dt_from_key(key) or _now_utc().strftime("%Y-%m-%d")

        articles_par_jour[pub_day] += 1
        articles_par_source[source] += 1

        tokens = [t for t in _tokenize(f"{titre} {contenu}") if _is_candidate_token(t) and not _is_stopword(t, lang)]
        keyword_counter.update(tokens)

        # Simple "topics": bigrams from title tokens only
        title_tokens = [t for t in _tokenize(titre) if _is_candidate_token(t) and not _is_stopword(t, lang)]
        for i in range(len(title_tokens) - 1):
            sujets_counter.update([f"{title_tokens[i]} {title_tokens[i+1]}"])

        processed += 1

    # Build gold payloads
    if gold_prefix:
        base_prefix = gold_prefix.rstrip("/")
        run_id_match = re.search(r"run_id=([^/]+)", base_prefix)
        dt_match = re.search(r"dt=(\d{4}-\d{2}-\d{2})", base_prefix)
        run_id = run_id_match.group(1) if run_id_match else _sha1(_now_utc_iso())[:12]
        gold_dt = dt_match.group(1) if dt_match else _now_utc().strftime("%Y-%m-%d")
    else:
        run_id = _sha1(_now_utc_iso())[:12]
        gold_dt = _now_utc().strftime("%Y-%m-%d")
        base_prefix = f"gold/dt={gold_dt}/run_id={run_id}"

    articles_par_jour_rows = [
        {"date": day, "nb_articles": count}
        for day, count in sorted(articles_par_jour.items(), key=lambda x: x[0])
    ]
    articles_par_source_rows = [
        {"source": src, "nb_articles": count}
        for src, count in sorted(articles_par_source.items(), key=lambda x: (-x[1], x[0]))
    ]
    mots_cles_rows = [
        {"mot_cle": w, "count": c}
        for w, c in keyword_counter.most_common(top_n_keywords)
    ]
    top_sujets_rows = [
        {"sujet": w, "count": c}
        for w, c in sujets_counter.most_common(top_n_keywords)
    ]

    keys_written: list[str] = []

    k1 = f"{base_prefix}/articles_par_jour.json"
    put_json(s3, bucket=bucket, key=k1, payload={"generated_at": _now_utc_iso(), "rows": articles_par_jour_rows})
    keys_written.append(k1)

    k2 = f"{base_prefix}/articles_par_source.json"
    put_json(s3, bucket=bucket, key=k2, payload={"generated_at": _now_utc_iso(), "rows": articles_par_source_rows})
    keys_written.append(k2)

    k3 = f"{base_prefix}/mots_cles.json"
    put_json(s3, bucket=bucket, key=k3, payload={"generated_at": _now_utc_iso(), "rows": mots_cles_rows})
    keys_written.append(k3)

    k4 = f"{base_prefix}/top_sujets.json"
    put_json(s3, bucket=bucket, key=k4, payload={"generated_at": _now_utc_iso(), "rows": top_sujets_rows})
    keys_written.append(k4)

    manifest_key = f"gold/_manifests/silver_to_gold_{run_id}.json"
    put_json(
        s3,
        bucket=bucket,
        key=manifest_key,
        payload={
            "run_id": run_id,
            "generated_at": _now_utc_iso(),
            "bucket": bucket,
            "silver_prefix": silver_prefix,
            "processed": processed,
            "rejected": rejected,
            "top_n_keywords": top_n_keywords,
            "gold_keys": keys_written,
        },
    )

    return {
        "run_id": run_id,
        "processed": processed,
        "rejected": rejected,
        "manifest_key": manifest_key,
        "gold_keys": keys_written,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="ETL Silver -> Gold (agrégations) (MinIO)")

    p.add_argument(
        "--silver-prefix",
        default="silver/",
        help="Préfixe S3 à lire (ex: silver/source=hespress/)",
    )
    p.add_argument(
        "--limit-keys",
        type=int,
        default=0,
        help="Limiter le nombre d'objets Silver parcourus (0 = pas de limite)",
    )
    p.add_argument(
        "--top-n",
        type=int,
        default=50,
        help="Top N mots-clés / sujets",
    )

    p.add_argument(
        "--gold-prefix",
        default=None,
        help="Préfixe Gold complet (ex: gold/dt=YYYY-MM-DD/run_id=XXXXXXXXXXXX). Si absent, il est généré automatiquement.",
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

    stats = aggregate_gold(
        s3=s3,
        bucket=cfg.bucket,
        silver_prefix=args.silver_prefix,
        limit_keys=args.limit_keys,
        top_n_keywords=args.top_n,
        gold_prefix=args.gold_prefix,
    )

    print(json.dumps({"status": "ok", "stats": stats}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
