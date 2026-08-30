import logging
import os
import sys
from datetime import datetime

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("quality_checks")
POSTGRES_CONN_ID = "postgres_ecommerce"
hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID) 

CHECKS = [
    ("dim_location has rows",       "SELECT COUNT(*) FROM dim_location",       lambda x: x > 0),
    ("dim_customer has rows",       "SELECT COUNT(*) FROM dim_customer",       lambda x: x > 0),
    ("dim_seller has rows",         "SELECT COUNT(*) FROM dim_seller",         lambda x: x > 0),
    ("dim_product has rows",        "SELECT COUNT(*) FROM dim_product",        lambda x: x > 0),
    ("dim_time has rows",           "SELECT COUNT(*) FROM dim_time",           lambda x: x > 0),
    ("fact has rows",               "SELECT COUNT(*) FROM fact_order_items",   lambda x: x > 0),
    ("fact > 90k rows",             "SELECT COUNT(*) FROM fact_order_items",   lambda x: x > 90_000),
    ("no NULL customer_key",
     "SELECT COUNT(*) FROM fact_order_items WHERE customer_key IS NULL",       lambda x: x == 0),
    ("no NULL product_key",
     "SELECT COUNT(*) FROM fact_order_items WHERE product_key IS NULL",        lambda x: x == 0),
    ("no NULL seller_key",
     "SELECT COUNT(*) FROM fact_order_items WHERE seller_key IS NULL",         lambda x: x == 0),
    ("no NULL order_date_key",
     "SELECT COUNT(*) FROM fact_order_items WHERE order_date_key IS NULL",     lambda x: x == 0),
    ("customer location_key resolved",
     "SELECT COUNT(*) FROM dim_customer WHERE location_key IS NULL",           lambda x: x == 0),
    ("seller location_key resolved",
     "SELECT COUNT(*) FROM dim_seller WHERE location_key IS NULL",             lambda x: x == 0),
    ("no negative prices",
     "SELECT COUNT(*) FROM fact_order_items WHERE price < 0",                  lambda x: x == 0),
    ("no negative freight",
     "SELECT COUNT(*) FROM fact_order_items WHERE freight_value < 0",          lambda x: x == 0),
    ("review scores 1–5",
     """SELECT COUNT(*) FROM fact_order_items
        WHERE review_score IS NOT NULL AND review_score NOT BETWEEN 1 AND 5""",
     lambda x: x == 0),
    ("no orphan date keys",
     """SELECT COUNT(*) FROM fact_order_items f
        LEFT JOIN dim_time dt ON f.order_date_key = dt.date_key
        WHERE f.order_date_key IS NOT NULL AND dt.date_key IS NULL""",
     lambda x: x == 0),
]


def run_checks() -> bool:
    conn = hook.get_conn()
    failed = []

    with conn.cursor() as cur:
        for name, sql, assertion in CHECKS:
            try:
                cur.execute(sql)
                result = cur.fetchone()[0]
                passed = assertion(result)
                level = logging.INFO if passed else logging.ERROR
                logger.log(level,
                    f"{'PASS' if passed else 'FAIL'} | {name} | result={result}")
                if not passed:
                    failed.append(name)
            except Exception as e:
                logger.error(f" ERROR | {name} | {e}")
                failed.append(name)

    conn.close()

    if failed:
        logger.error(f"\n{len(failed)} check(s) failed:")
        for f in failed:
            logger.error(f"  — {f}")
        return False

    logger.info(f"\n All {len(CHECKS)} quality checks passed")
    return True


def main():
    started = datetime.now()
    ok      = run_checks()
    logger.info(f"Quality checks finished in {datetime.now() - started}")
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
