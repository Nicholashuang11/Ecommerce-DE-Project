import logging
import os

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("refresh_views")

DB_CONFIG = {
    "host":     "commerce-postgres",  
    "port":     5432,                       
    "dbname":   "ecommerce_dw",
    "user":     "commerce",
    "password": "commerce",
}

SQL_PATH =  "/opt/airflow/src/analytics_views.sql"



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
