import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

connectionid = "olistdbid"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("refresh_views")


def get_postgres_hook():
    return PostgresHook(postgres_conn_id=connectionid)


SQL_PATH = "/opt/airflow/src/analytics_views.sql"


def refresh():
    with open(SQL_PATH, "r") as f:
        sql = f.read()

    hook = get_postgres_hook()
    conn = hook.get_conn()
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
