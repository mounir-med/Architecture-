# Architecture

```mermaid
flowchart LR
  %% 2026-05-10: Added Mermaid architecture diagram covering Scrapers -> Kafka -> MinIO (Bronze/Silver/Gold/Speed) -> PostgreSQL -> Superset, orchestrated by Airflow.

  subgraph Sources[Sources de presse]
    H[Hespress]
    A[Akhbarona]
    G[Goud]
    B[Barlamane]
    J[Al Jazeera Arabic]
    BB[BBC Arabic]
    R[Reuters]
  end

  subgraph Scrapers[Scrapers (Python)]
    S1[hespress_scraper.py]
    S2[akhbarona_scraper.py]
    S3[goud_scraper.py]
    S4[barlamane_scraper.py]
    S5[aljazeera_scraper.py]
    S6[bbc_arabic_scraper.py]
    S7[reuters_scraper.py]
  end

  subgraph Kafka[Kafka]
    T[(Topic: media.articles)]
  end

  subgraph MinIO[MinIO (Data Lake)]
    BR[(Bronze: bronze/)]
    SI[(Silver: silver/)]
    GO[(Gold: gold/)]
    SP[(Speed: speed/)]
  end

  subgraph ETL[ETL (Python)]
    E1[bronze_to_silver.py]
    E2[silver_to_gold.py]
  end

  subgraph DWH[Warehouse (PostgreSQL)]
    P[(PostgreSQL: media)]
    TJ[articles_par_jour]
    TS[articles_par_source]
    TM[mots_cles]
    TT[top_sujets]
    TTH[articles_par_theme]
    TPA[articles_par_pays]
  end

  subgraph BI[BI]
    SUP[Superset Dashboards]
  end

  subgraph Orchestration[Orchestration]
    AF[Airflow DAG: media_pipeline_hourly]
  end

  subgraph SpeedLayer[Speed layer]
    KC[Kafka Consumer (kafka_consumer.py)]
  end

  H --> S1
  A --> S2
  G --> S3
  B --> S4
  J --> S5
  BB --> S6
  R --> S7

  S1 --> T
  S2 --> T
  S3 --> T
  S4 --> T
  S5 --> T
  S6 --> T
  S7 --> T

  AF --> S1
  AF --> S2
  AF --> S3
  AF --> S4
  AF --> S5
  AF --> S6
  AF --> S7

  AF --> BR

  BR --> E1 --> SI
  SI --> E2 --> GO

  KC --> T
  KC --> SP

  GO --> P

  P --> TJ
  P --> TS
  P --> TM
  P --> TT
  P --> TTH
  P --> TPA

  P --> SUP

  %% Loader
  L[warehouse/loader.py]
  AF --> L
  GO --> L --> P
```
