from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2
import os

TMDB_API_KEY = os.getenv("TMDB_API_KEY")

# Fetch TMDB language catalog and return (full_rows, codes)
def fetch_language_catalog():
    if not TMDB_API_KEY:
        raise RuntimeError("TMDB_API_KEY is not set in Airflow env")
    url = f"https://api.themoviedb.org/3/configuration/languages?api_key={TMDB_API_KEY}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    languages = resp.json() or []
    seen, codes = set(), []
    for row in languages:
        code = (row or {}).get("iso_639_1") or ""
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    return languages, codes

# Yield normalized movies for a language and date
def fetch_movies(language_code: str, release_date: str):
    import logging
    if not release_date:
        raise RuntimeError("release_date is empty; set RELEASE_DATE or remove it to default to today")
    page, yielded = 1, 0
    while True:
        url = (
            f"https://api.themoviedb.org/3/discover/movie"
            f"?api_key={TMDB_API_KEY}"
            f"&with_original_language={language_code}"
            f"&primary_release_date.gte={release_date}"
            f"&primary_release_date.lte={release_date}"
            f"&sort_by=primary_release_date.asc"
            f"&include_adult=false"
            f"&page={page}"
        )
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break
        for movie in results:
            rd = movie.get("release_date") or ""
            if rd == release_date:
                yielded += 1
                yield {
                    "tmdb_id": movie.get("id"),
                    "language": movie.get("original_language"),
                    "original_title": movie.get("original_title"),
                    "release_date": rd,
                    "title": movie.get("title"),
                    "overview": movie.get("overview"),
                    "popularity": movie.get("popularity"),
                    "adult": movie.get("adult"),
                }
        if page >= data.get("total_pages", 1):
            break
        page += 1
    logging.info(f"[fetch_movies] language={language_code} date={release_date} yielded={yielded}")

# Main ETL: upsert language catalog, then load movies
def store_movies():
    import logging
    if not TMDB_API_KEY:
        raise RuntimeError("TMDB_API_KEY is not set in Airflow env")

    release_date = os.getenv("RELEASE_DATE") or datetime.utcnow().date().isoformat()
    logging.info(f"[store_movies] Using release_date={release_date}")

    conn = psycopg2.connect(
        host="postgres",
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    conn.autocommit = False
    cur = conn.cursor()

    # Create catalog table first (FK target), then movies table (FK source)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS languages (
            code VARCHAR(10) PRIMARY KEY,
            english_name TEXT,
            name TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            tmdb_id INTEGER PRIMARY KEY,
            language VARCHAR(10) REFERENCES languages(code),
            original_title TEXT,
            release_date DATE,
            title TEXT,
            overview TEXT,
            popularity NUMERIC,
            adult BOOLEAN,
            fetched_at TIMESTAMP DEFAULT NOW()
        );
    """)
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_movies_release_date ON movies (release_date);""")

    # Load/refresh language catalog
    languages, LANGUAGE_CODES = fetch_language_catalog()
    upsert_language_sql = """
        INSERT INTO languages (code, english_name, name)
        VALUES (%s, %s, %s)
        ON CONFLICT (code) DO UPDATE SET
            english_name = EXCLUDED.english_name,
            name = EXCLUDED.name
    """
    for lang in languages:
        code = (lang or {}).get("iso_639_1") or ""
        english_name = (lang or {}).get("english_name") or ""
        native_name = (lang or {}).get("name") or ""
        if code:
            cur.execute(upsert_language_sql, (code, english_name, native_name))
    conn.commit()

    insert_sql = """
        INSERT INTO movies (tmdb_id, language, original_title, release_date, title, overview, popularity, adult)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tmdb_id) DO NOTHING
    """

    total = 0
    for code in LANGUAGE_CODES:
        try:
            count = 0
            for m in fetch_movies(code, release_date):
                if not m["release_date"]:
                    continue
                cur.execute(
                    insert_sql,
                    (
                        m["tmdb_id"],
                        m["language"],
                        m["original_title"],
                        m["release_date"],
                        m["title"],
                        m["overview"],
                        m["popularity"],
                        m["adult"],
                    ),
                )
                count += 1
            conn.commit()
            total += count
            logging.info(f"[store_movies] inserted={count} for language={code}")
        except Exception:
            logging.exception(f"[store_movies] language={code} failed, rolling back")
            conn.rollback()

    cur.close()
    conn.close()
    logging.info(f"[store_movies] DONE total_inserted={total} for date={release_date}")

default_args = {"start_date": datetime(2024, 1, 1)}

with DAG(
    dag_id="movies_etl",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    store_movies_task = PythonOperator(
        task_id="store_movies",
        python_callable=store_movies,
    )
    store_movies_task