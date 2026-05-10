# Présentation du Projet — Plateforme Big Data Media

## 1. Contexte et Objectifs

Les médias publient chaque jour des milliers d'articles en ligne. Ces données constituent une source d'information précieuse pour :

- **Identifier les tendances d'actualité** en temps réel et sur le long terme
- **Analyser les thèmes dominants** par source, langue et région
- **Suivre les événements** au fil des publications
- **Détecter les anomalies** et signaux faibles dans la couverture médiatique

Ce projet met en place une **plateforme Big Data complète** capable de collecter automatiquement des articles de presse depuis plusieurs sites d'actualité marocains et internationaux, puis de stocker, transformer et analyser ces données afin d'identifier les tendances médiatiques.

---

## 2. Architecture Globale

La solution repose sur une **architecture distribuée en couches** (architecture Médaillon) intégrant :

```
Scrapers Web → Ingestion (Batch + Streaming) → Data Lake (Bronze/Silver/Gold) → Data Warehouse → Visualisation
```

### Couches fonctionnelles

| Couche | Rôle | Technologie |
|--------|------|-------------|
| Collecte | Scraping automatisé des sites de presse | Python + BeautifulSoup |
| Ingestion Batch | Collecte périodique toutes les heures | Apache Airflow |
| Ingestion Streaming | Événements en temps réel | Apache Kafka |
| Data Lake Bronze | Stockage brut des articles | MinIO (S3-compatible) |
| Data Lake Silver | Données nettoyées et normalisées | Python + langdetect |
| Data Lake Gold | Agrégations analytiques | Python |
| Data Warehouse | Tables analytiques pour la BI | PostgreSQL |
| Visualisation | Tableaux de bord interactifs | Apache Superset |
| Orchestration | Planification et supervision | Apache Airflow |
| Déploiement | Conteneurisation | Docker + Docker Compose |

---

## 3. Choix Technologiques Justifiés

### Python
Langage principal pour les scrapers, les ETL et les scripts utilitaires. Choisi pour sa richesse en bibliothèques (BeautifulSoup, boto3, psycopg2, langdetect), sa lisibilité et sa facilité de maintenance.

### Apache Kafka
Broker de messages pour l'ingestion streaming. Kafka garantit la durabilité des messages, le découplage entre producteurs et consommateurs, et la scalabilité horizontale. Utilisé avec Zookeeper pour la coordination du cluster.

### MinIO
Stockage objet S3-compatible déployé en local (ou on-premise). MinIO est utilisé comme Data Lake : il stocke les fichiers JSON partitionnés par source et par date, permettant une organisation hiérarchique de type `bronze/source=.../dt=YYYY-MM-DD/`. Alternative légère à AWS S3 ou HDFS.

### Apache Airflow
Orchestrateur de pipelines de données. Le DAG `media_pipeline_hourly` planifie et enchaîne toutes les étapes du pipeline batch (scraping → bronze → silver → gold → warehouse) avec gestion des dépendances, des retries et de la supervision.

### PostgreSQL
Base de données relationnelle pour le Data Warehouse. Stocke les tables analytiques agrégées (articles par jour, par source, par thème, par pays, mots-clés, sujets) consommées par Superset.

### Apache Superset
Outil de Business Intelligence open-source pour la visualisation. Connecté à PostgreSQL, il expose des dashboards interactifs (évolution temporelle, classements, tendances).

### Docker + Docker Compose
Tous les composants sont conteneurisés. Docker Compose orchestre l'ensemble des services avec leurs dépendances, volumes persistants et variables d'environnement.

---

## 4. Description des Couches

### 4.1 Collecte — Web Scraping

**Fichiers :** `scrapers/`

Sept scrapers sont développés en Python avec BeautifulSoup :

| Scraper | Site | Langue | Pays |
|---------|------|--------|------|
| `hespress_scraper.py` | hespress.com | Arabe/Français | Maroc |
| `akhbarona_scraper.py` | akhbarona.com | Arabe | Maroc |
| `barlamane_scraper.py` | barlamane.com | Arabe | Maroc |
| `goud_scraper.py` | goud.ma | Arabe | Maroc |
| `aljazeera_scraper.py` | aljazeera.net | Arabe | Qatar |
| `bbc_arabic_scraper.py` | bbc.com/arabic | Arabe | Royaume-Uni |
| `reuters_scraper.py` | reuters.com | Anglais | USA |

Chaque scraper collecte les champs : `titre`, `url`, `date_publication`, `categorie`, `auteur`, `contenu`, `source`.

### 4.2 Ingestion

**Fichiers :** `ingestion/`

Deux modes d'ingestion coexistent :

**Batch** — Airflow lance les scrapers toutes les heures. Les articles sont écrits en JSON dans MinIO (couche Bronze) via `datalake/lake_writer.py`.

**Streaming** — `ingestion/kafka_producer.py` publie chaque article comme événement sur le topic Kafka `media.articles`. `ingestion/kafka_consumer.py` (Speed Layer) consomme ces événements en temps réel, les nettoie, détecte la langue et les écrit dans MinIO sous `speed/dt=.../`.

### 4.3 Data Lake — Architecture Médaillon

**Fichiers :** `datalake/`, `etl/`

**Bronze — Données brutes**
- Chemin : `bronze/source=<source>/dt=YYYY-MM-DD/articles/<sha1_url>.json`
- Contenu : articles JSON tels que collectés, sans validation
- Principe : conserver l'historique complet, jamais de suppression

**Silver — Données nettoyées**
- Chemin : `silver/source=<source>/dt=YYYY-MM-DD/articles/<sha1_url>.json`
- Transformations : suppression des balises HTML, normalisation des espaces, détection de langue (`langdetect`), déduplication par URL
- Validation : titre non vide, date présente, contenu > 100 caractères, URL valide
- Les articles rejetés sont consignés dans un manifest de run

**Gold — Données agrégées**
- Chemin : `gold/dt=YYYY-MM-DD/run_id=<id>/<table>.json`
- Contenu : `articles_par_jour`, `articles_par_source`, `articles_par_theme`, `articles_par_pays`, `mots_cles`, `top_sujets`

### 4.4 ETL / ELT

**Fichiers :** `etl/bronze_to_silver.py`, `etl/silver_to_gold.py`

- `bronze_to_silver.py` : lit tous les objets Bronze, normalise, valide, écrit en Silver. Produit un manifest de run (run_id, clés écrites, rejets détaillés).
- `silver_to_gold.py` : lit tous les objets Silver, agrège par jour/source/thème/pays, extrait les mots-clés (stop words filtrés) et les sujets dominants, écrit les fichiers Gold.

### 4.5 Orchestration

**Fichiers :** `orchestration/dags/media_pipeline_dag.py`

Le DAG Airflow `media_pipeline_hourly` enchaîne :

1. `compute_gold_prefix` — calcule le préfixe Gold du run
2. `scrape_<source>` × 7 — scraping en parallèle
3. `bronze_write_<source>` × 7 — écriture Bronze en parallèle
4. `bronze_to_silver` — ETL Silver (après tous les bronze_write)
5. `silver_to_gold` — agrégation Gold
6. `load_gold_to_postgres` — chargement en Data Warehouse

### 4.6 Data Warehouse

**Fichiers :** `warehouse/schema.sql`, `warehouse/loader.py`

Tables PostgreSQL alimentées par UPSERT depuis les fichiers Gold :

| Table | Clé primaire | Description |
|-------|-------------|-------------|
| `articles_par_jour` | date | Nombre d'articles publiés par jour |
| `articles_par_source` | source | Nombre d'articles par site |
| `articles_par_theme` | theme | Nombre d'articles par catégorie/thème |
| `articles_par_pays` | pays | Nombre d'articles par pays d'origine |
| `mots_cles` | mot_cle | Fréquence des mots-clés |
| `top_sujets` | sujet | Fréquence des sujets dominants |

### 4.7 Visualisation

**Fichiers :** `superset/dashboards/media_dashboard.json`, `superset/init.sh`

Le dashboard **Media Analytics** dans Apache Superset expose :

- Évolution du nombre d'articles par jour (graphique linéaire temporel)
- Nombre d'articles par source (bar chart)
- Articles par thème (bar chart horizontal)
- Articles par pays (bar chart)
- Top mots-clés (tableau trié par fréquence)
- Top sujets (tableau trié par fréquence)

---

## 5. Qualité des Données

### Règles de validation (couche Silver)

| Règle | Champ | Condition de rejet |
|-------|-------|-------------------|
| Titre obligatoire | `titre` | Vide après nettoyage HTML |
| Date obligatoire | `date_publication` | Absente ou vide |
| Contenu suffisant | `contenu` | Longueur ≤ 100 caractères |
| URL valide | `url` | Ne respecte pas le format http/https |
| Déduplication | `url` | URL déjà vue dans le run courant |

### Dimensions de qualité couvertes

- **Complétude** : vérification de la présence des champs obligatoires
- **Validité** : format URL, longueur minimale du contenu
- **Déduplication** : élimination des doublons par URL dans chaque run

### Traçabilité

Chaque run ETL produit un manifest JSON stocké dans MinIO :
- `silver/_manifests/bronze_to_silver_<run_id>.json` : articles écrits, rejetés avec causes
- `gold/_manifests/silver_to_gold_<run_id>.json` : statistiques d'agrégation

---

## 6. Gouvernance des Données

### Data Catalog

Le fichier `docs/data_catalog.md` documente toutes les couches du Data Lake :
- Schéma des champs avec types et descriptions
- Chemins S3 et conventions de nommage
- Règles de validation par couche
- Exemples de documents JSON

### Traçabilité du pipeline

- Chaque article Silver référence sa clé Bronze d'origine (`bronze_key`)
- Chaque run est identifié par un `run_id` unique (SHA1 du timestamp)
- Les manifests conservent l'historique complet des traitements

---

## 7. Déploiement

L'ensemble de la plateforme est déployé via Docker Compose en un seul fichier `docker-compose.yml` :

```bash
docker-compose up --build
```

### Services déployés

| Service | Image | Port |
|---------|-------|------|
| Zookeeper | confluentinc/cp-zookeeper:7.6.1 | 2181 |
| Kafka | confluentinc/cp-kafka:7.6.1 | 9092, 29092 |
| MinIO | minio/minio | 9000, 9001 |
| PostgreSQL | postgres:16 | 5432 |
| Airflow Webserver | custom (docker/airflow/) | 8080 |
| Airflow Scheduler | custom (docker/airflow/) | — |
| Apache Superset | apache/superset:4.0.2 | 8088 |
| Scraper | custom (docker/scraper/) | — |
| Kafka Consumer | custom (docker/scraper/) | — |

Volumes persistants : `minio_data`, `postgres_data`, `airflow_logs`, `superset_home`.
