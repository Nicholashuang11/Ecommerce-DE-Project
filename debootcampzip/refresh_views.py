```python
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

logger = logging.getLogger("refresh_views")

POSTGRES_CONN_ID = "postgres_ecommerce"
SQL_PATH = "/opt/airflow/src/analytics_views.sql"
hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def refresh():
    with open(SQL_PATH, "r") as f:
        sql = f.read()

    conn = hook.get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute(sql)

        conn.commit()
        logger.info("Analytics views refreshed successfully")

    except Exception:
        conn.rollback()
        logger.exception("Failed to refresh analytics views")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    refresh()
```
