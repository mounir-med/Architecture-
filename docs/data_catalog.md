# Data Catalog

## Vue d'ensemble
Le projet utilise une architecture médaillon :
- **Bronze** : articles bruts issus des scrapers (JSON)
- **Silver** : articles normalisés (nettoyage HTML, whitespace), langue détectée
- **Gold** : agrégations analytiques (mots-clés, sujets, volumes)
- **Warehouse** : tables PostgreSQL consommées par Superset

---

## Bronze (MinIO)
### Description
Stockage des données brutes, au format JSON, écrites par `datalake/lake_writer.py`.

### Chemins
- `bronze/source=<source>/dt=YYYY-MM-DD/articles/<sha1_url>.json` (si `--split-per-article`)
- ou `bronze/source=<source>/dt=YYYY-MM-DD/<HHMMSS>_<batch_id>.json` (batch)

### Champs (article)
- `titre` (string) : titre brut
- `url` (string) : URL canonique
- `date_publication` (string|null) : date/heure brute (souvent ISO)
- `categorie` (string|null) : catégorie/section
- `auteur` (string|null) : auteur (optionnel)
- `contenu` (string) : contenu (peut contenir HTML)
- `source` (string) : nom du site (ex: `hespress`)

### Règles de validation
Aucune validation stricte en Bronze (données brutes).

### Exemple JSON (objet par article)
```json
{
  "ingested_at": "2026-05-09T18:00:00+00:00",
  "source": "hespress",
  "layer": "bronze",
  "article": {
    "titre": "...",
    "url": "https://...",
    "date_publication": "...",
    "categorie": "...",
    "auteur": null,
    "contenu": "<p>...</p>",
    "source": "hespress"
  }
}
```

---

## Silver (MinIO)
### Description
Normalisation et validation via `etl/bronze_to_silver.py`.
- suppression HTML
- normalisation des espaces
- détection de langue via `langdetect`

### Chemins
- `silver/source=<source>/dt=YYYY-MM-DD/articles/<sha1_url>.json`

### Champs (article)
- `titre` (string) : texte nettoyé
- `url` (string) : URL
- `date_publication` (string|null)
- `categorie` (string|null)
- `auteur` (string|null)
- `contenu` (string) : texte nettoyé
- `source` (string)
- `langue` (string|null) : code langue (ex: `fr`, `ar`, `en`)
- `normalized_at` (string) : timestamp UTC

### Règles de validation (Silver)
- `titre` non vide
- `date_publication` présent
- `contenu` longueur > 100
- `url` valide http/https
- `auteur` est optionnel (ne rejette pas l'article)

### Exemple JSON
```json
{
  "layer": "silver",
  "bronze_key": "bronze/source=hespress/dt=2026-05-09/articles/<sha1>.json",
  "ingested_at": "2026-05-09T18:05:00+00:00",
  "article": {
    "titre": "...",
    "url": "https://...",
    "date_publication": "...",
    "categorie": "...",
    "auteur": null,
    "contenu": "...",
    "source": "hespress",
    "langue": "fr",
    "normalized_at": "2026-05-09T18:05:00+00:00"
  }
}
```

---

## Gold (MinIO)
### Description
Agrégations via `etl/silver_to_gold.py`.

### Chemins
- `gold/dt=YYYY-MM-DD/run_id=<id>/articles_par_jour.json`
- `gold/dt=YYYY-MM-DD/run_id=<id>/articles_par_source.json`
- `gold/dt=YYYY-MM-DD/run_id=<id>/mots_cles.json`
- `gold/dt=YYYY-MM-DD/run_id=<id>/top_sujets.json`

### Formats
- `articles_par_jour.json` : `rows[]` avec `date`, `nb_articles`
- `articles_par_source.json` : `rows[]` avec `source`, `nb_articles`
- `mots_cles.json` : `rows[]` avec `mot_cle`, `count`
- `top_sujets.json` : `rows[]` avec `sujet`, `count`

### Exemple JSON (mots_cles)
```json
{
  "generated_at": "2026-05-09T19:00:00+00:00",
  "rows": [
    {"mot_cle": "maroc", "count": 120},
    {"mot_cle": "economie", "count": 85}
  ]
}
```

---

## Speed (MinIO)
### Description
Traitement temps réel via `ingestion/kafka_consumer.py` :
- nettoyage identique (HTML -> texte + normalisation)
- détection langue
- métriques glissantes en mémoire (5 min) loggées toutes les 60 s

### Chemins
- `speed/dt=YYYY-MM-DD/<sha1_url>.json`

### Exemple JSON
```json
{
  "layer": "speed",
  "kafka": {"topic": "media.articles", "partition": 0, "offset": 12, "timestamp": 1715280000000, "key": "https://..."},
  "processed_at": "2026-05-09T18:10:00+00:00",
  "article": {
    "titre": "...",
    "url": "https://...",
    "date_publication": "...",
    "categorie": "...",
    "auteur": null,
    "contenu": "...",
    "source": "hespress",
    "langue": "fr",
    "normalized_at": "2026-05-09T18:10:00+00:00"
  }
}
```

---

## Warehouse (PostgreSQL)
### Description
Tables SQL (voir `warehouse/schema.sql`) alimentées par `warehouse/loader.py`.

### Tables
- `articles_par_jour(date DATE PRIMARY KEY, nb_articles INTEGER, updated_at TIMESTAMPTZ)`
- `articles_par_source(source TEXT PRIMARY KEY, nb_articles INTEGER, updated_at TIMESTAMPTZ)`
- `mots_cles(mot_cle TEXT PRIMARY KEY, count INTEGER, updated_at TIMESTAMPTZ)`
- `top_sujets(sujet TEXT PRIMARY KEY, count INTEGER, updated_at TIMESTAMPTZ)`
