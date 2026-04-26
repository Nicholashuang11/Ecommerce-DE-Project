"""
refresh_views.py
Re-executes analytics_views.sql after every successful load.
"""
import logging
import os

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("refresh_views")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "olist_dw"),
    "user":     os.getenv("POSTGRES_USER",     "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

SQL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "sql", "analytics_views.sql"
)


def refresh():
    with open(SQL_PATH, "r") as f:
        sql = f.read()

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("Refresh analytics view")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    refresh()
