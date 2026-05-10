# Documentation Technique — Installation et Utilisation

## Prérequis

| Outil | Version minimale | Vérification |
|-------|-----------------|-------------|
| Docker | 24.0+ | `docker --version` |
| Docker Compose | 2.20+ | `docker compose version` |
| Python | 3.11+ (optionnel, pour exécution locale) | `python --version` |
| Git | 2.x | `git --version` |

**Ressources recommandées :** 8 Go RAM, 20 Go d'espace disque libre.

---

## 1. Cloner le dépôt

```bash
git clone <url-du-depot> Projet2
cd Projet2
```

---

## 2. Démarrage de l'infrastructure

```bash
docker-compose up --build
```

Cette commande :
- Construit les images Docker personnalisées (Airflow, Scraper)
- Lance tous les services dans l'ordre des dépendances
- Monte les volumes persistants (`minio_data`, `postgres_data`, `airflow_logs`, `superset_home`)

> **Premier démarrage :** Compter 3 à 5 minutes pour que tous les services soient opérationnels, notamment Airflow (initialisation de la base de données) et Superset.

Pour lancer en arrière-plan :
```bash
docker-compose up --build -d
```

---

## 3. Vérifier la disponibilité des services

```bash
# Voir l'état de tous les conteneurs
docker-compose ps

# Voir les logs d'un service spécifique
docker-compose logs -f airflow-webserver
docker-compose logs -f kafka
docker-compose logs -f minio
```

Attendre que tous les services affichent le statut `running` ou `healthy`.

---

## 4. Accès aux interfaces web

| Service | URL | Identifiants |
|---------|-----|-------------|
| Apache Airflow | http://localhost:8080 | admin / admin |
| Apache Superset | http://localhost:8088 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | media / media (DB: media) |

---

## 5. Configuration des variables d'environnement

Les variables sont définies dans `docker-compose.yml`. Pour les modifier, éditez le fichier ou créez un fichier `.env` à la racine :

```env
# MinIO
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
DATALAKE_BUCKET=media-datalake

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=media.articles
KAFKA_GROUP_ID=media-consumer
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_ENABLE_AUTO_COMMIT=true

# PostgreSQL
PGHOST=postgres
PGPORT=5432
PGDATABASE=media
PGUSER=media
PGPASSWORD=media
```

---

## 6. Lancer manuellement un scraper

### Depuis l'intérieur du conteneur Scraper

```bash
# Accéder au conteneur
docker-compose exec scraper bash

# Lancer le scraper Hespress (20 articles max)
python /app/scrapers/hespress_scraper.py --max-articles 20 --output /tmp/hespress.json

# Vérifier le résultat
cat /tmp/hespress.json | python -m json.tool | head -50
```

### Scraper disponibles

```bash
# Marocains
python /app/scrapers/hespress_scraper.py    --max-articles 20 --output /tmp/hespress.json
python /app/scrapers/akhbarona_scraper.py   --max-articles 20 --output /tmp/akhbarona.json
python /app/scrapers/barlamane_scraper.py   --max-articles 20 --output /tmp/barlamane.json
python /app/scrapers/goud_scraper.py        --max-articles 20 --output /tmp/goud.json

# Internationaux
python /app/scrapers/aljazeera_scraper.py   --max-articles 20 --output /tmp/aljazeera.json
python /app/scrapers/bbc_arabic_scraper.py  --max-articles 20 --output /tmp/bbc.json
python /app/scrapers/reuters_scraper.py     --max-articles 20 --output /tmp/reuters.json
```

### Écrire en Bronze manuellement

```bash
python /app/datalake/lake_writer.py \
  --input-json /tmp/hespress.json \
  --source hespress \
  --split-per-article
```

---

## 7. Déclencher le DAG Airflow manuellement

### Via l'interface web

1. Ouvrir http://localhost:8080
2. Se connecter avec `admin / admin`
3. Chercher le DAG `media_pipeline_hourly`
4. Cliquer sur le bouton ▶ **Trigger DAG**
5. Suivre l'exécution dans la vue **Graph** ou **Grid**

### Via la CLI Airflow

```bash
docker-compose exec airflow-webserver \
  airflow dags trigger media_pipeline_hourly
```

### Vérifier l'état du DAG

```bash
docker-compose exec airflow-webserver \
  airflow dags list-runs --dag-id media_pipeline_hourly
```

---

## 8. Exécuter l'ETL manuellement

### Bronze → Silver

```bash
docker-compose exec scraper \
  python /app/etl/bronze_to_silver.py \
  --bronze-prefix bronze/ \
  --default-source hespress
```

### Silver → Gold

```bash
docker-compose exec scraper \
  python /app/etl/silver_to_gold.py \
  --silver-prefix silver/ \
  --top-n 50
```

### Charger Gold dans PostgreSQL

```bash
docker-compose exec scraper \
  python /app/warehouse/loader.py \
  --gold-prefix gold/dt=2026-05-10/run_id=<run_id> \
  --pg-host postgres \
  --pg-port 5432 \
  --pg-db media \
  --pg-user media \
  --pg-password media
```

> Remplacer `<run_id>` par la valeur visible dans les logs de `silver_to_gold.py`.

---

## 9. Accéder aux dashboards Superset

### Importer le dashboard

1. Ouvrir http://localhost:8088
2. Connecter la base de données PostgreSQL :
   - Menu → **Settings** → **Database Connections** → **+ Database**
   - Type : **PostgreSQL**
   - URI : `postgresql+psycopg2://media:media@postgres:5432/media`
3. Importer le dashboard :
   - Menu → **Dashboards** → **Import**
   - Charger le fichier `superset/dashboards/media_dashboard.json`

### Dashboards disponibles

| Dashboard | Description |
|-----------|-------------|
| Évolution articles/jour | Graphique linéaire temporel du volume d'articles |
| Articles par source | Bar chart des volumes par site |
| Articles par thème | Bar chart des catégories les plus couvertes |
| Articles par pays | Bar chart des pays d'origine |
| Top mots-clés | Tableau des 50 mots-clés les plus fréquents |
| Top sujets | Tableau des 50 sujets dominants |

---

## 10. Vérifier les données dans MinIO

1. Ouvrir http://localhost:9001
2. Se connecter avec `minioadmin / minioadmin`
3. Explorer le bucket `media-datalake`
4. Naviguer dans les préfixes : `bronze/`, `silver/`, `gold/`, `speed/`

---

## 11. Vérifier les données dans PostgreSQL

```bash
# Se connecter à PostgreSQL
docker-compose exec postgres psql -U media -d media

# Requêtes de vérification
SELECT COUNT(*) FROM articles_par_jour;
SELECT COUNT(*) FROM articles_par_source;
SELECT COUNT(*) FROM articles_par_theme;
SELECT COUNT(*) FROM articles_par_pays;
SELECT COUNT(*) FROM mots_cles;
SELECT COUNT(*) FROM top_sujets;

-- Top 5 sources
SELECT source, nb_articles FROM articles_par_source ORDER BY nb_articles DESC LIMIT 5;
```

---

## 12. Arrêter l'infrastructure

```bash
# Arrêter les conteneurs (volumes conservés)
docker-compose down

# Arrêter et supprimer les volumes (ATTENTION : supprime toutes les données)
docker-compose down -v
```

---

## 13. Troubleshooting

### Kafka : "Connection refused" ou timeout

```bash
# Vérifier que Kafka et Zookeeper sont démarrés
docker-compose ps kafka zookeeper

# Voir les logs Kafka
docker-compose logs kafka | tail -50

# Solution : attendre 30 secondes après le démarrage de Zookeeper avant Kafka
```

### MinIO : bucket introuvable

```bash
# Vérifier que le bucket existe
docker-compose exec minio mc ls local/

# Créer le bucket manuellement si nécessaire
docker-compose exec minio mc mb local/media-datalake
```

### Airflow : DAG en erreur

```bash
# Voir les logs de la tâche en échec
docker-compose logs airflow-scheduler | grep ERROR

# Réinitialiser l'état d'une tâche
docker-compose exec airflow-webserver \
  airflow tasks clear media_pipeline_hourly -t scrape_hespress --yes
```

### Superset : "Database connection error"

- Vérifier que PostgreSQL est démarré : `docker-compose ps postgres`
- Utiliser le hostname `postgres` (et non `localhost`) dans la chaîne de connexion
- URI correcte : `postgresql+psycopg2://media:media@postgres:5432/media`

### Scraper : aucun article collecté

```bash
# Tester la connectivité réseau depuis le conteneur
docker-compose exec scraper curl -I https://www.hespress.com/

# Vérifier les logs du scraper
docker-compose logs scraper
```
