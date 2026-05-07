import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from kafka import KafkaProducer


@dataclass(frozen=True)
class ProducerConfig:
    bootstrap_servers: str
    topic: str
    acks: str
    linger_ms: int
    retries: int
    request_timeout_ms: int


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_serializer(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def build_producer(cfg: ProducerConfig) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[s.strip() for s in cfg.bootstrap_servers.split(",") if s.strip()],
        value_serializer=_json_serializer,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        acks=cfg.acks,
        linger_ms=cfg.linger_ms,
        retries=cfg.retries,
        request_timeout_ms=cfg.request_timeout_ms,
        api_version_auto_timeout_ms=10000,
    )


def load_articles_from_json(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    articles = payload.get("articles")
    if not isinstance(articles, list):
        raise ValueError("Format JSON invalide: clé 'articles' manquante ou non-liste")

    out: list[dict[str, Any]] = []
    for a in articles:
        if isinstance(a, dict):
            out.append(a)
    return out


def publish_articles(
    producer: KafkaProducer,
    topic: str,
    source_file: str,
    articles: list[dict[str, Any]],
    throttle_ms: int,
) -> dict[str, Any]:
    sent = 0
    failed = 0

    for a in articles:
        url = str(a.get("url", "")).strip()
        if not url:
            failed += 1
            continue

        event = {
            "event_type": "article.scraped",
            "event_time": _now_utc_iso(),
            "source_file": source_file,
            "article": a,
        }

        try:
            # key=url: utile pour partitionnement stable et dédup côté consumer
            producer.send(topic, key=url, value=event)
            sent += 1
        except Exception:
            failed += 1

        if throttle_ms > 0:
            time.sleep(throttle_ms / 1000)

    producer.flush(timeout=30)

    return {
        "topic": topic,
        "sent": sent,
        "failed": failed,
        "total": len(articles),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Kafka Producer - Publier des articles (JSON) vers un topic")
    p.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers, ex: localhost:9092",
    )
    p.add_argument(
        "--topic",
        default="media.articles",
        help="Nom du topic Kafka",
    )
    p.add_argument(
        "--input-json",
        required=True,
        help="Chemin du JSON produit par le scraper (ex: hespress_articles.json)",
    )
    p.add_argument(
        "--acks",
        default="all",
        choices=["0", "1", "all"],
        help="Niveau d'acknowledgement Kafka",
    )
    p.add_argument(
        "--linger-ms",
        type=int,
        default=50,
        help="Linger ms (batching)",
    )
    p.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retries",
    )
    p.add_argument(
        "--request-timeout-ms",
        type=int,
        default=30000,
        help="Timeout des requêtes Kafka",
    )
    p.add_argument(
        "--throttle-ms",
        type=int,
        default=0,
        help="Pause entre messages (anti-burst), en ms",
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    articles = load_articles_from_json(args.input_json)

    cfg = ProducerConfig(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        acks=args.acks,
        linger_ms=args.linger_ms,
        retries=args.retries,
        request_timeout_ms=args.request_timeout_ms,
    )

    producer = build_producer(cfg)
    stats = publish_articles(
        producer=producer,
        topic=args.topic,
        source_file=args.input_json,
        articles=articles,
        throttle_ms=args.throttle_ms,
    )

    print(json.dumps({"status": "ok", "stats": stats}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
