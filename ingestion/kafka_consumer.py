import json
import os
import re
import hashlib
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from kafka import KafkaConsumer
from langdetect import LangDetectException, detect


@dataclass(frozen=True)
class ConsumerConfig:
    bootstrap_servers: str
    topic: str
    group_id: str
    auto_offset_reset: str
    enable_auto_commit: bool


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


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _clean_text(text: Any) -> str:
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


def normalize_article(raw: dict[str, Any], default_source: str) -> dict[str, Any]:
    titre = _clean_text(raw.get("titre", ""))
    url = str(raw.get("url", "")).strip()
    date_publication = raw.get("date_publication")
    categorie = _clean_text(raw.get("categorie")) if raw.get("categorie") is not None else None
    auteur = _clean_text(raw.get("auteur")) if raw.get("auteur") is not None else None
    auteur = auteur if auteur else None
    contenu = _clean_text(raw.get("contenu", ""))
    source = _clean_text(raw.get("source", "")) or default_source

    langue = detect_language(f"{titre}. {contenu}")

    return {
        "titre": titre,
        "url": url,
        "date_publication": date_publication,
        "categorie": categorie,
        "auteur": auteur,
        "contenu": contenu,
        "source": source,
        "langue": langue,
        "normalized_at": _now_utc_iso(),
    }


def build_consumer(cfg: ConsumerConfig) -> KafkaConsumer:
    return KafkaConsumer(
        cfg.topic,
        bootstrap_servers=[s.strip() for s in cfg.bootstrap_servers.split(",") if s.strip()],
        group_id=cfg.group_id,
        auto_offset_reset=cfg.auto_offset_reset,
        enable_auto_commit=cfg.enable_auto_commit,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if isinstance(b, (bytes, bytearray)) else b,
        consumer_timeout_ms=1000,
        api_version_auto_timeout_ms=10000,
    )


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


def speed_article_key(dt: str, url: str) -> str:
    h = _sha1(url)
    return f"speed/dt={dt}/{h}.json"


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[\w\u0600-\u06FF']+", text.lower(), flags=re.UNICODE)


def _is_candidate_token(t: str) -> bool:
    if len(t) < 3:
        return False
    if t.isdigit():
        return False
    return True


def run() -> int:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "media.articles")

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_region = os.getenv("MINIO_REGION", "us-east-1")
    bucket = os.getenv("DATALAKE_BUCKET", "media-datalake")

    group_id = os.getenv("KAFKA_GROUP_ID", "media-speed-consumer")
    auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
    enable_auto_commit = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "true").lower() in {"1", "true", "yes"}

    default_source = os.getenv("DEFAULT_SOURCE", "unknown")

    cfg = ConsumerConfig(
        bootstrap_servers=kafka_bootstrap,
        topic=topic,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
    )

    s3_cfg = S3Config(
        endpoint_url=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        region_name=minio_region,
        bucket=bucket,
    )

    s3 = build_s3_client(s3_cfg)
    ensure_bucket(s3, s3_cfg.bucket)

    consumer = build_consumer(cfg)

    window = timedelta(minutes=5)
    events: deque[tuple[datetime, str, list[str]]] = deque()

    last_metrics_log = time.time()

    try:
        while True:
            now = _now_utc()

            while events and (now - events[0][0]) > window:
                events.popleft()

            try:
                msg_pack = consumer.poll(timeout_ms=1000)
            except Exception as e:
                print(json.dumps({"status": "error", "stage": "kafka.poll", "error": f"{type(e).__name__}: {e}"}, ensure_ascii=False))
                continue

            for _, msgs in msg_pack.items():
                for msg in msgs:
                    try:
                        value = msg.value
                        if not isinstance(value, dict):
                            raise ValueError("message value is not a dict")

                        raw_article = value.get("article")
                        if not isinstance(raw_article, dict):
                            raise ValueError("missing/invalid article")

                        normalized = normalize_article(raw_article, default_source=default_source)
                        url = str(normalized.get("url", "")).strip()
                        source = str(normalized.get("source", "")).strip() or default_source

                        dt = now.strftime("%Y-%m-%d")
                        key = speed_article_key(dt=dt, url=url or _sha1(_now_utc_iso()))

                        record = {
                            "layer": "speed",
                            "kafka": {
                                "topic": getattr(msg, "topic", None),
                                "partition": getattr(msg, "partition", None),
                                "offset": getattr(msg, "offset", None),
                                "timestamp": getattr(msg, "timestamp", None),
                                "key": msg.key,
                            },
                            "processed_at": _now_utc_iso(),
                            "article": normalized,
                        }

                        put_json(s3, bucket=s3_cfg.bucket, key=key, payload=record)

                        tokens = [t for t in _tokenize(f"{normalized.get('titre', '')} {normalized.get('contenu', '')}") if _is_candidate_token(t)]
                        events.append((now, source, tokens))
                    except (json.JSONDecodeError, UnicodeDecodeError, ValueError, TypeError) as e:
                        print(
                            json.dumps(
                                {
                                    "status": "error",
                                    "stage": "deserialize_or_validate",
                                    "error": f"{type(e).__name__}: {e}",
                                    "topic": getattr(msg, "topic", None),
                                    "partition": getattr(msg, "partition", None),
                                    "offset": getattr(msg, "offset", None),
                                },
                                ensure_ascii=False,
                            )
                        )
                    except Exception as e:
                        print(
                            json.dumps(
                                {
                                    "status": "error",
                                    "stage": "process_message",
                                    "error": f"{type(e).__name__}: {e}",
                                    "topic": getattr(msg, "topic", None),
                                    "partition": getattr(msg, "partition", None),
                                    "offset": getattr(msg, "offset", None),
                                },
                                ensure_ascii=False,
                            )
                        )

            if time.time() - last_metrics_log >= 60:
                by_source: dict[str, int] = defaultdict(int)
                kw = Counter[str]()

                for _, src, toks in events:
                    by_source[src] += 1
                    kw.update(toks)

                metrics = {
                    "status": "ok",
                    "ts": _now_utc_iso(),
                    "window_sec": int(window.total_seconds()),
                    "articles_per_source": dict(sorted(by_source.items(), key=lambda x: (-x[1], x[0]))),
                    "top_keywords": [{"mot": w, "count": c} for w, c in kw.most_common(10)],
                }
                print(json.dumps(metrics, ensure_ascii=False))
                last_metrics_log = time.time()

    except KeyboardInterrupt:
        print(json.dumps({"status": "stopped", "reason": "KeyboardInterrupt"}, ensure_ascii=False))
        return 0
    finally:
        try:
            consumer.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(run())
