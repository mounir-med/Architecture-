# Projet Media Big Data — Collecte & Analyse d'Articles de Presse

## Description
Ce projet met en place une architecture Big Data pour **collecter**, **ingérer**, **stocker** et **analyser** des articles de presse.

Le pipeline suit un modèle **médaillon** :
- Bronze : données brutes (JSON) dans MinIO
- Silver : données nettoyées/normalisées + langue détectée
- Gold : agrégations (comptages, mots-clés, sujets, thèmes, pays)
- Warehouse : tables PostgreSQL pour la BI (Superset)

## Architecture (flux)

Un diagramme Mermaid plus complet est disponible dans `docs/architecture.md`.

```
              +--------------------+
              |     Scrapers       |
              | (multi-sites JSON) |
              +---------+----------+
                        |
                        v
              +--------------------+
              | Kafka Producer     |
              | topic: media.articles
              +---------+----------+
                        |
         (batch)        |        (temps réel)
     Airflow DAG        |     Kafka Consumer
   @hourly orchestration|     speed layer
        |               |           |
        v               v           v
+---------------+   +-------------------+
| MinIO Bronze  |   | MinIO Speed       |
| bronze/...    |   | speed/dt=...      |
+-------+-------+   +---------+---------+
        |
        v
+---------------+
| ETL Silver    |
| silver/...    |
+-------+-------+
        |
        v
+---------------+
| ETL Gold      |
| gold/...      |
+-------+-------+
        |
        v
+-------------------+
| PostgreSQL (DWH)   |
| tables analytics   |
+---------+---------+
          |
          v
+-------------------+
| Superset Dashboards|
+-------------------+
```

## Prérequis
- Docker
- Docker Compose

### Ports utilisés
- Kafka : `9092` (container), `29092` (host)
- Zookeeper : `2181`
- MinIO API : `9000`
- MinIO Console : `9001`
- PostgreSQL : `5432`
- Airflow : `8080`
- Superset : `8088`

## Démarrage (pas à pas)
1. Lancer l'infrastructure :

   ```bash
   docker-compose up --build
   ```

2. Accès services :
- Airflow : http://localhost:8080
- Superset : http://localhost:8088
- MinIO Console : http://localhost:9001

## Services Docker (docker-compose)
- `zookeeper` : coordination Kafka
- `kafka` : broker Kafka
- `minio` : data lake S3 compatible
- `postgres` : data warehouse (schéma auto-init via `warehouse/schema.sql`)
- `airflow-init`, `airflow-webserver`, `airflow-scheduler` : orchestration
- `superset` : BI
- `scraper` : image utilitaire (scrapers/ingestion/etl/warehouse)
- `kafka-consumer` : speed layer temps réel (topic `media.articles` → MinIO `speed/`)

## Modules
- `scrapers/`
  - Scrapers multi-sites (sortie JSON uniforme)
  - Sources supportées :
    - `hespress_scraper.py` → `hespress`
    - `akhbarona_scraper.py` → `akhbarona`
    - `goud_scraper.py` → `goud`
    - `barlamane_scraper.py` → `barlamane`
    - `aljazeera_scraper.py` → `aljazeera`
    - `bbc_arabic_scraper.py` → `bbc_arabic`
    - `reuters_scraper.py` → `reuters`

- `ingestion/`
  - `kafka_producer.py` : publie des articles JSON sur Kafka (topic `media.articles`)
  - `kafka_consumer.py` : speed layer (nettoyage + métriques + écriture MinIO)

- `datalake/`
  - `lake_writer.py` : écrit en Bronze dans MinIO (`bronze/source=.../dt=...`)

- `etl/`
  - `bronze_to_silver.py` : nettoyage (HTML->texte), normalisation, langue (`langdetect`)
  - `silver_to_gold.py` : agrégations (articles/jour, articles/source, mots-clés, sujets, thèmes, pays)

- `warehouse/`
  - `schema.sql` : schéma PostgreSQL
  - `loader.py` : charge Gold -> PostgreSQL

### Tables Warehouse (PostgreSQL)
- `articles_par_jour`
- `articles_par_source`
- `mots_cles`
- `top_sujets`
- `articles_par_theme`
- `articles_par_pays`

- `orchestration/dags/`
  - `media_pipeline_dag.py` : pipeline Airflow (horaire)

## Variables d'environnement
### Kafka
- `KAFKA_BOOTSTRAP_SERVERS` (ex: `kafka:9092`)
- `KAFKA_TOPIC` (par défaut `media.articles`)
- `KAFKA_GROUP_ID` (consumer)
- `KAFKA_AUTO_OFFSET_RESET` (`latest`/`earliest`)
- `KAFKA_ENABLE_AUTO_COMMIT` (`true`/`false`)

### MinIO
- `MINIO_ENDPOINT` (ex: `http://minio:9000`)
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_REGION` (défaut `us-east-1`)
- `DATALAKE_BUCKET` (défaut `media-datalake`)

### Scrapers / Consumer
- `DEFAULT_SOURCE` (fallback si source manquante)

## Documentation
- `docs/data_catalog.md`
- `docs/architecture.md`
