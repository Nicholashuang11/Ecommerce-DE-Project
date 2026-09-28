import logging
import os
from airflow.hooks.base import BaseHook

connectionid= "olistdbid"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("refresh_views")
def connectdb(connid):
    return BaseHook.get_connection(connid)


SQL_PATH =  "/opt/airflow/src/analytics_views.sql"



def refresh():
    with open(SQL_PATH, "r") as f:
        sql = f.read()

    conn = connectdb(connectionid)
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
