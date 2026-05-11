import psycopg2
from datetime import datetime, timedelta
import random

def seed():
    conn = psycopg2.connect(
        dbname="media",
        user="media",
        password="media",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()

    print("Seeding articles_par_jour...")
    for i in range(30):
        dt = (datetime.now() - timedelta(days=i)).date()
        cur.execute(
            "INSERT INTO articles_par_jour (date, nb_articles) VALUES (%s, %s) ON CONFLICT (date) DO UPDATE SET nb_articles = EXCLUDED.nb_articles",
            (dt, random.randint(10, 100))
        )

    print("Seeding articles_par_source...")
    sources = ["Hespress", "Akhbarona", "Goud", "Barlamane", "Aljazeera", "BBC Arabic", "Reuters"]
    for source in sources:
        cur.execute(
            "INSERT INTO articles_par_source (source, nb_articles) VALUES (%s, %s) ON CONFLICT (source) DO UPDATE SET nb_articles = EXCLUDED.nb_articles",
            (source, random.randint(100, 1000))
        )

    print("Seeding mots_cles...")
    keywords = ["Maroc", "Politique", "Economie", "Sport", "Santé", "Education", "Technologie", "Climat", "Justice", "Culture"]
    for kw in keywords:
        cur.execute(
            "INSERT INTO mots_cles (mot_cle, count) VALUES (%s, %s) ON CONFLICT (mot_cle) DO UPDATE SET count = EXCLUDED.count",
            (kw, random.randint(50, 500))
        )

    print("Seeding top_sujets...")
    subjects = ["Gouvernement", "Football", "IA", "Inflation", "Sahara", "Tourisme", "Agriculture", "Startups"]
    for sj in subjects:
        cur.execute(
            "INSERT INTO top_sujets (sujet, count) VALUES (%s, %s) ON CONFLICT (sujet) DO UPDATE SET count = EXCLUDED.count",
            (sj, random.randint(30, 300))
        )

    print("Seeding articles_par_theme...")
    themes = ["Actualité", "Sports", "Business", "Lifestyle", "International", "Opinion"]
    for theme in themes:
        cur.execute(
            "INSERT INTO articles_par_theme (theme, nb_articles) VALUES (%s, %s) ON CONFLICT (theme) DO UPDATE SET nb_articles = EXCLUDED.nb_articles",
            (theme, random.randint(200, 800))
        )

    print("Seeding articles_par_pays...")
    countries = ["Maroc", "France", "Espagne", "USA", "Chine", "Algérie", "Tunisie", "Egypte"]
    for country in countries:
        cur.execute(
            "INSERT INTO articles_par_pays (pays, nb_articles) VALUES (%s, %s) ON CONFLICT (pays) DO UPDATE SET nb_articles = EXCLUDED.nb_articles",
            (country, random.randint(50, 400))
        )

    conn.commit()
    cur.close()
    conn.close()
    print("Done seeding data!")

if __name__ == "__main__":
    seed()
