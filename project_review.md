# Project Review — Big Data Media Platform
**Plateforme Big Data pour l'Analyse des Tendances Médiatiques**

> **Authors:** Elhamss Med Mounir · Mouad Salah
> **Institution:** École Supérieure de Technologie — Département Informatique & Big Data
> **Academic Year:** 2025–2026
> **Repository:** https://github.com/mounir-med/Architecture-

---

## Table of Contents

1. [Project Summary](#1-project-summary)
2. [Problem Statement & Objectives](#2-problem-statement--objectives)
3. [Architecture Overview](#3-architecture-overview)
4. [Technology Stack](#4-technology-stack)
5. [Data Sources & Web Scraping](#5-data-sources--web-scraping)
6. [Data Ingestion](#6-data-ingestion)
7. [Data Lake — Medallion Architecture](#7-data-lake--medallion-architecture)
8. [ETL Transformations](#8-etl-transformations)
9. [Orchestration — Apache Airflow](#9-orchestration--apache-airflow)
10. [Data Warehouse — PostgreSQL](#10-data-warehouse--postgresql)
11. [Visualization — Apache Superset](#11-visualization--apache-superset)
12. [Deployment — Docker](#12-deployment--docker)
13. [Data Governance & Quality](#13-data-governance--quality)
14. [Strengths & Highlights](#14-strengths--highlights)
15. [Identified Limitations & Improvement Axes](#15-identified-limitations--improvement-axes)
16. [Conclusion](#16-conclusion)

---

## 1. Project Summary

This project delivers a complete, production-grade **Big Data platform** designed for automated media monitoring. It ingests news articles from 6 press sources (Moroccan and international), processes them through a multi-layer ETL pipeline, stores them in a distributed object storage system, and exposes analytical dashboards for end users.

The platform is built on a **Lambda architecture**, combining:
- a **Batch Layer** for historical and scheduled processing (hourly Airflow DAG),
- a **Speed Layer** for real-time streaming (Apache Kafka consumer),
- a **Serving Layer** for analytical queries (PostgreSQL + Apache Superset).

The entire stack is containerized and deployable via a single `docker-compose up --build` command.

---

## 2. Problem Statement & Objectives

### Problem

Media organizations publish thousands of articles daily across political, economic, sports, and cultural topics. Manually monitoring this volume is impractical. Organizations need tools that can automatically collect, process, and analyze this data to extract value — detecting trending topics, identifying dominant themes, tracking events in real time, and combating misinformation.

### Objectives

| # | Objective | Status |
|---|-----------|--------|
| 1 | Automatically collect articles from multiple Moroccan and international sources | ✅ Implemented |
| 2 | Set up a hybrid ingestion pipeline (hourly batch + real-time streaming via Kafka) | ✅ Implemented |
| 3 | Store raw data in a MinIO Data Lake following the Medallion architecture (Bronze / Silver / Gold) | ✅ Implemented |
| 4 | Apply ETL transformations: cleaning, normalization, language detection, aggregations | ✅ Implemented |
| 5 | Feed a PostgreSQL Data Warehouse for analytical querying | ✅ Implemented |
| 6 | Orchestrate the full pipeline with Apache Airflow | ✅ Implemented |
| 7 | Visualize key indicators in Apache Superset dashboards | ✅ Implemented |
| 8 | Deploy the platform in a containerized Docker environment | ✅ Implemented |

---

## 3. Architecture Overview

The solution follows a **Lambda Architecture** pattern:

```
 Press Sources (6 outlets)
        │
        ▼
 Python Scrapers (BeautifulSoup + Requests)
        │
        ▼
 Apache Kafka  ──────────────────────────────────────────┐
 topic: media.articles                                    │
        │                                                 │
        │ BATCH LAYER (hourly)                            │ SPEED LAYER (real-time)
        ▼                                                 ▼
 MinIO Bronze (raw)                               MinIO Speed Zone
        │                                          (Kafka metadata + cleaned)
        ▼
 MinIO Silver (cleaned + language detected)
        │
        ▼
 MinIO Gold (aggregated views)
        │
        └──────────────────────────┐
                                   ▼
                         PostgreSQL Data Warehouse
                                   │
                                   ▼
                          Apache Superset Dashboards
```

All components run inside Docker containers managed by Docker Compose, with persistent volumes for data durability.

### Key architectural decisions

**MinIO over HDFS** — MinIO provides full S3-compatible API access via boto3, runs on-premise via Docker, and avoids the complexity of a Hadoop cluster for an academic project of this scale.

**Kafka as the central bus** — Decouples scraping from both the batch and the speed processing layers. A single `kafka_producer.py` feeds both paths simultaneously with no duplication of scraping logic.

**PostgreSQL for both Airflow metadata and the Data Warehouse** — A pragmatic choice that reduces the infrastructure footprint while satisfying analytical query needs at this data volume.

**SHA-1 of URL as article ID** — Guarantees idempotency across the entire pipeline. Re-ingesting the same article overwrites rather than duplicates, making re-processing safe.

---

## 4. Technology Stack

| Component | Technology | Version | Role |
|-----------|------------|---------|------|
| Scraping | Python + BeautifulSoup + Requests | — | Automated article collection |
| Message broker | Apache Kafka (Confluent) | 7.6.1 | Real-time streaming ingestion |
| Cluster coordination | Apache Zookeeper | 7.6.1 | Kafka cluster management |
| Data Lake | MinIO | 2024-10-02 | S3-compatible object storage |
| ETL | Python + boto3 + langdetect | — | Transformation and enrichment |
| Orchestration | Apache Airflow | — | Pipeline scheduling and monitoring |
| Data Warehouse | PostgreSQL | 16 | Structured analytical storage |
| Visualization | Apache Superset | 4.0.2 | Dashboards and KPIs |
| Containerization | Docker / Docker Compose | — | Deployment and portability |

---

## 5. Data Sources & Web Scraping

### Covered sources

| Scraper | Website | Language | Script |
|---------|---------|----------|--------|
| Hespress | hespress.com | Arabic / French | `hespress_scraper.py` |
| Akhbarona | akhbarona.com | Arabic | `akhbarona_scraper.py` |
| Goud | goud.ma | Arabic | `goud_scraper.py` |
| Barlamane | barlamane.com | Arabic | `barlamane_scraper.py` |
| Al Jazeera | aljazeera.net | Arabic | `aljazeera_scraper.py` |
| BBC Arabic | bbc.com/arabic | Arabic | `bbc_arabic_scraper.py` |

### Normalized article schema

Every scraper produces a uniform JSON object:

```json
{
  "titre":            "string  — article title (required)",
  "url":              "string  — canonical URL (required)",
  "date_publication": "string  — ISO datetime or null",
  "categorie":        "string  — section/category or null",
  "auteur":           "string  — author name or null",
  "contenu":          "string  — article body, may contain HTML (required)",
  "source":           "string  — site identifier e.g. hespress (required)"
}
```

### Scraping process

Each scraper operates in two phases:

1. **Listing phase** — crawls the homepage or category pages to collect URLs of recent articles (configurable via `--max-articles`).
2. **Collection phase** — for each URL, performs an HTTP request with a realistic `User-Agent` header, parses the HTML DOM, extracts the relevant fields, and validates the URL. Rate-limiting delays between requests avoid overloading target servers.

Articles are written to `/tmp/<source>_articles.json` before being ingested into the Data Lake by `lake_writer.py`.

---

## 6. Data Ingestion

### 6.1 Batch ingestion

The batch ingestion is orchestrated by the Airflow DAG `media_pipeline_hourly`. This DAG runs every hour (`@hourly`) and executes the 6 scrapers in parallel. Each scraper is followed by a `bronze_write` task that persists the data into MinIO.

The DAG also computes a unique `gold_prefix` (date + SHA-1 of the timestamp) to avoid naming collisions in the Gold layer when concurrent runs occur.

### 6.2 Streaming ingestion (Kafka)

Two components handle streaming ingestion:

- **`kafka_producer.py`** — publishes JSON articles to the `media.articles` topic. Configured via environment variables: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`.
- **`kafka_consumer.py`** (Speed Layer) — continuously consumes the topic, applies the same cleaning pipeline as `bronze_to_silver.py`, detects language, and persists each article into the `speed/` zone of MinIO. It also computes rolling metrics (articles/5 min, language distribution) logged every 60 seconds.

### 6.3 Kafka configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Broker address |
| `KAFKA_TOPIC` | `media.articles` | Topic name |
| `KAFKA_GROUP_ID` | — | Consumer group ID |
| `KAFKA_AUTO_OFFSET_RESET` | `latest` | Offset reading strategy |
| `KAFKA_ENABLE_AUTO_COMMIT` | `true` | Automatic offset commit |

---

## 7. Data Lake — Medallion Architecture

The Data Lake is hosted on **MinIO** and follows the **Medallion architecture** (Bronze / Silver / Gold), with an additional Speed zone for real-time data.

### Bronze — Raw data

The entry point of the Data Lake. Articles are stored in their exact raw JSON form as collected by the scrapers, with no transformation.

- **Path:** `bronze/source=<source>/dt=YYYY-MM-DD/articles/<sha1_url>.json`
- No strict validation — all data is preserved, even incomplete records.
- Added metadata: `ingested_at` (UTC timestamp), `source`, `layer='bronze'`.

### Silver — Cleaned data

Produced by `bronze_to_silver.py`. Transformations applied:

- HTML tag removal via regex `<[^>]+>` → space.
- Whitespace normalization: `\s+` → single space.
- Automatic language detection via `langdetect` on a 20+ character sample.
- Multi-criteria quality validation: non-empty title, valid URL (http/https regex), content ≥ 100 characters, publication date present.

Articles failing validation are logged and excluded. Valid articles are written to `silver/source=<source>/dt=YYYY-MM-DD/articles/<sha1_url>.json` with added fields `langue` and `normalized_at`.

### Gold — Aggregated data

Produced by `silver_to_gold.py`. Contains analytical aggregations ready for loading into the Data Warehouse.

| Gold file | Content | Fields |
|-----------|---------|--------|
| `articles_par_jour.json` | Daily article volume | `date`, `nb_articles` |
| `articles_par_source.json` | Volume by source | `source`, `nb_articles` |
| `mots_cles.json` | Top-N keywords | `mot_cle`, `count` |
| `top_sujets.json` | Top identified subjects | `sujet`, `count` |
| `articles_par_theme.json` | Distribution by theme | `theme`, `nb_articles` |
| `articles_par_pays.json` | Distribution by country (via language) | `pays`, `nb_articles` |

Each Gold file includes a `generated_at` timestamp for traceability. Paths are partitioned by date and `run_id` for partial re-processing support.

### Speed zone — Real-time processing

Articles processed in real time by the Kafka Consumer. Path: `speed/dt=YYYY-MM-DD/<sha1_url>.json`. Includes full Kafka metadata: `topic`, `partition`, `offset`, `timestamp`, and `processed_at`.

---

## 8. ETL Transformations

### Bronze → Silver pipeline

`bronze_to_silver.py` uses boto3 to recursively iterate over all objects under the `bronze/` prefix. For each article:

1. Extract raw JSON from MinIO.
2. HTML cleaning: regex `<[^>]+>` → space.
3. Whitespace normalization: `\s+` → space.
4. Language detection on a 20+ character sample.
5. Multi-criteria validation (title, URL, content, date).
6. Write Silver JSON to MinIO with new fields.

### Silver → Gold pipeline

`silver_to_gold.py` reads all Silver objects and computes analytical aggregations using Python `Counter` and `defaultdict` structures. The `--top-n` parameter (default: 50) controls the number of entries in keyword and subject tables.

Theme detection uses normalized `categorie` field values. Country classification uses detected language as a proxy: `ar` → Monde arabe, `fr` → France/Maghreb, `en` → International.

### Data quality controls

| Dimension | Control | Action on failure |
|-----------|---------|-------------------|
| Completeness | Non-empty title and content | Excluded from Silver |
| Validity | URL matches http/https format | Excluded from Silver |
| Validity | Content > 100 characters | Excluded from Silver |
| Consistency | Publication date present | Excluded from Silver |
| Consistency | Non-empty source field | Default value injected |
| Uniqueness | SHA-1 of URL as unique ID | Overwritten on re-processing |

---

## 9. Orchestration — Apache Airflow

### DAG: `media_pipeline_hourly`

The main DAG orchestrates the entire batch pipeline. Scheduled at `@hourly` from January 1, 2025. `catchup=False` prevents retrospective execution of missed runs.

### Dependency graph

```
compute_gold_prefix
        │
        ├── scrape_hespress ──► bronze_write_hespress ──┐
        ├── scrape_akhbarona ─► bronze_write_akhbarona ─┤
        ├── scrape_goud ──────► bronze_write_goud ───────┤
        ├── scrape_barlamane ─► bronze_write_barlamane ──┤  (all 7 in parallel)
        ├── scrape_aljazeera ─► bronze_write_aljazeera ──┤
        └── scrape_bbc ──────► bronze_write_bbc ─────────┘
                                                          │
                                                   bronze_to_silver
                                                          │
                                                   silver_to_gold
                                                          │
                                               load_gold_to_postgres
```

The graph guarantees that Silver transformation does not begin until all 6 Bronze writes are complete, preventing partial data from entering the Silver layer.

### Configuration

- **Executor:** LocalExecutor
- **Metadata DB:** PostgreSQL (shared with the Data Warehouse)
- **DAG directory:** mounted via Docker volume at `/opt/airflow/dags`
- **MinIO connection parameters:** injected via environment variables in each `BashOperator` task

---

## 10. Data Warehouse — PostgreSQL

### Schema

The schema is initialized automatically at container startup via `warehouse/schema.sql`, mounted in `/docker-entrypoint-initdb.d/`.

| Table | Primary key | Business columns | Description |
|-------|-------------|-----------------|-------------|
| `articles_par_jour` | `date DATE` | `nb_articles INTEGER` | Daily article volume |
| `articles_par_source` | `source TEXT` | `nb_articles INTEGER` | Volume by press source |
| `mots_cles` | `mot_cle TEXT` | `count INTEGER` | Keyword frequency |
| `top_sujets` | `sujet TEXT` | `count INTEGER` | Subject ranking |
| `articles_par_theme` | `theme TEXT` | `nb_articles INTEGER` | Thematic distribution |
| `articles_par_pays` | `pays TEXT` | `nb_articles INTEGER` | Geographic distribution |

Each table includes an `updated_at` (`TIMESTAMPTZ DEFAULT NOW()`) column updated at every load cycle for traceability.

### Loading strategy

`warehouse/loader.py` implements an **upsert** strategy (`INSERT ... ON CONFLICT DO UPDATE`) for all tables. This idempotent approach allows safe re-execution of the load step — essential in an Airflow context with retry mechanisms.

---

## 11. Visualization — Apache Superset

### Configuration

Apache Superset 4.0.2 is deployed in production mode, connecting to the PostgreSQL Data Warehouse via SQLAlchemy. Dashboards and datasets are initialized automatically (via `init.sh` and manual injection scripts) to ensure a stable visualization layer.

### Dashboards

| Dashboard | Visualization type | Data source |
|-----------|-------------------|-------------|
| News trends | Time-series line chart | `articles_par_jour` |
| Distribution by source | Bar / pie chart | `articles_par_source` |
| Keyword cloud | Word cloud (weighted by count) | `mots_cles` |
| Top subjects | Ranked bar chart | `top_sujets` |
| Geographic map | Choropleth map | `articles_par_pays` |
| Thematic breakdown | Stacked bar / donut chart | `articles_par_theme` |

---

## 12. Deployment — Docker

### Services

| Service | Image | Port(s) | Role |
|---------|-------|---------|------|
| `zookeeper` | `confluentinc/cp-zookeeper:7.6.1` | 2181 | Kafka coordination |
| `kafka` | `confluentinc/cp-kafka:7.6.1` | 9092, 29092 | Message broker |
| `minio` | `minio/minio:RELEASE.2024-10-02...` | 9000, 9001 | Data Lake |
| `postgres` | `postgres:16` | 5432 | Data Warehouse + Airflow DB |
| `airflow-init` | custom | — | Airflow initialization |
| `airflow-webserver` | custom | 8080 | Airflow UI |
| `airflow-scheduler` | custom | — | DAG scheduler |
| `superset` | `apache/superset:4.0.2` | 8088 | BI dashboards |
| `scraper` | custom | — | Scrapers, ETL, Warehouse loader |
| `kafka-consumer` | custom | — | Speed Layer consumer |

### Startup

```bash
docker-compose up --build
```

### Access URLs

| Interface | URL | Credentials |
|-----------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8088 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | db: media, user: media |

### Persistent volumes

| Volume | Content |
|--------|---------|
| `minio_data` | Data Lake (Bronze, Silver, Gold, Speed) |
| `postgres_data` | Data Warehouse + Airflow metadata |
| `airflow_logs` | DAG execution logs |
| `superset_home` | Superset configuration and metadata |

### Optional Kubernetes deployment

For production-scale deployment, the platform can be ported to Kubernetes via Helm Charts, enabling:
- Horizontal auto-scaling of scrapers and Airflow workers.
- High availability via pod replication.
- Persistent data via PersistentVolumeClaims.
- Secret management via Kubernetes Secrets and ConfigMaps.

---

## 13. Data Governance & Quality

### Traceability

Traceability is embedded at every layer via metadata fields:

| Layer | Traceability fields |
|-------|-------------------|
| Bronze | `ingested_at`, `source`, `layer='bronze'` |
| Silver | `bronze_key` (source S3 key), `normalized_at`, `layer='silver'`, `langue` |
| Gold | `generated_at` |
| Speed | `topic`, `partition`, `offset`, `timestamp`, `processed_at` |
| Data Warehouse | `updated_at` on all tables |

### Article deduplication

Articles are identified by the **SHA-1 hash of their canonical URL**. This mechanism guarantees idempotency: if an article is collected multiple times (due to re-processing or scraping overlap), it is overwritten rather than duplicated, preserving Data Lake consistency.

### Data catalog

`docs/data_catalog.md` documents the full data catalog: storage paths, JSON schemas with types and descriptions, validation rules, and data examples for each layer.

---

## 14. Strengths & Highlights

**End-to-end completeness** — The project covers the full data engineering lifecycle: collection, ingestion, transformation, quality, orchestration, storage, warehousing, and visualization. Few academic projects achieve this scope.

**Lambda architecture correctly implemented** — The batch and speed layers are truly independent, both fed from the same Kafka topic. The serving layer consolidates both paths. This is a textbook Lambda implementation.

**Medallion architecture with strong quality gates** — The Bronze/Silver/Gold layering is cleanly implemented with explicit quality rules at the Silver boundary. Articles are never silently corrupted — they are either validated and promoted, or excluded and logged.

**Idempotent pipeline** — SHA-1-based IDs and upsert loading strategies mean every step can be safely re-executed without side effects. This is a production-grade design principle rarely found in student projects.

**Fully containerized and reproducible** — A single command deploys all 10 services. No manual configuration is required. This demonstrates real DevOps maturity.

**Multilingual coverage** — The platform handles Arabic, French, and English sources simultaneously, with automatic language detection enriching each article.

---

## 15. Identified Limitations & Improvement Axes

### Current limitations

**Language-based country classification** — Using the detected language as a proxy for country (e.g., `ar` → Monde arabe) is a simplification. A single Arabic article could originate from Morocco, Egypt, or Saudi Arabia. Named Entity Recognition (NER) would produce more accurate geographic attribution.

**No NLP pipeline** — Keyword extraction relies on simple word counting without stopword removal or stemming for Arabic and French. This produces noisy keyword clouds. A proper NLP pipeline (spaCy, CAMeL Tools for Arabic) would significantly improve analytical value.

**Shared PostgreSQL instance** — Using the same PostgreSQL server for both Airflow metadata and the Data Warehouse creates a potential single point of failure. In production, these should be separated.

**No monitoring stack** — There is no Prometheus/Grafana layer for pipeline observability. DAG failures are only visible in the Airflow UI; there are no alerts or SLA tracking.

**Static scraping logic** — Scrapers are tightly coupled to the DOM structure of their target sites. A single site redesign will break the corresponding scraper without warning.

### Recommended improvement axes

1. **Add an NLP pipeline** — Arabic and French text classification using pre-trained models (AraBERT, CamemBERT) for automatic theme detection and fake news flagging.
2. **Introduce a monitoring stack** — Prometheus metrics exporters on Airflow and Kafka, visualized in Grafana, with alerting on SLA breaches.
3. **Kubernetes deployment** — For production scale, migrate from Docker Compose to a Kubernetes cluster with Helm Charts, enabling auto-scaling and high availability.
4. **Expand source coverage** — Add RSS feed-based ingestion for sources that support it, reducing DOM coupling and scraper fragility.
5. **Add data freshness SLAs** — Define and enforce maximum acceptable lag between article publication and availability in the Gold layer.

---

## 16. Conclusion

This project successfully delivers a complete, modern Big Data platform for media trend analysis. The architecture demonstrates a solid command of distributed data engineering principles: Lambda architecture, Medallion layering, event-driven streaming, idempotent pipelines, and containerized deployment.

The combination of Kafka-based real-time ingestion, Airflow-orchestrated batch processing, and MinIO-based object storage constitutes a production-credible data foundation. The 6-dashboard Superset layer provides immediate analytical value from the collected data.

The main improvement opportunities lie in NLP enrichment (moving beyond simple keyword counting toward semantic analysis), operational observability (monitoring and alerting), and infrastructure maturity (Kubernetes, separated database instances). These are natural next steps that would elevate the platform from an excellent academic project to a deployable production system.

---

*Elhamss Med Mounir & Mouad Salah — 2025/2026*
*École Supérieure de Technologie — Architecture de Données Big Data*
