import argparse
import logging
from datetime import datetime

import numpy as np
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook


connectionid = "olistdbid"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger("load")


def get_postgres_hook():
    return PostgresHook(postgres_conn_id=connectionid)


def to_native(val):
    if val is None:
        return None
    if isinstance(val, float) and (val != val):
        return None
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        return float(val)
    if isinstance(val, np.bool_):
        return bool(val)
    return val


def df_to_records(df: pd.DataFrame) -> list:
    return [
        tuple(to_native(v) for v in row)
        for row in df.itertuples(index=False)
    ]


def load_dimension(
    parquet_path: str,
    table: str,
    conflict_col: str,
) -> int:

    logger.info(f"Loading {table}")

    df = pd.read_parquet(parquet_path)

    if df.empty:
        logger.warning(f"No rows found in table {table}")
        return 0

    cols = list(df.columns)
    rows = df_to_records(df)

    hook = get_postgres_hook()

    hook.upsert_rows(
        table=table,
        rows=rows,
        target_fields=cols,
        conflict_fields=[conflict_col],
        commit_every=1000,
    )

    logger.info(
        f"Done load {table}. Total rows: {len(df):,}"
    )

    return len(df)


def load_dim_customer(parquet_path: str) -> int:

    logger.info("Loading dim_customer")

    df = pd.read_parquet(parquet_path)

    if df.empty:
        logger.warning("No rows found for dim_customer")
        return 0

    hook = get_postgres_hook()

    hook.upsert_rows(
        table="dim_customer",
        rows=df_to_records(df),
        target_fields=[
            "customer_id",
            "customer_unique_id",
            "zip_code_prefix",
        ],
        conflict_fields=["customer_id"],
        update_fields=[
            "customer_unique_id",
            "zip_code_prefix",
        ],
        commit_every=1000,
    )

    logger.info("success insert dim_customer")

    return len(df)


def load_dim_seller(parquet_path: str) -> int:

    logger.info("Loading dim_seller")

    df = pd.read_parquet(parquet_path)

    if df.empty:
        logger.warning("No row found for dim_seller")
        return 0

    hook = get_postgres_hook()

    hook.upsert_rows(
        table="dim_seller",
        rows=df_to_records(df),
        target_fields=[
            "seller_id",
            "zip_code_prefix",
        ],
        conflict_fields=["seller_id"],
        update_fields=[
            "zip_code_prefix",
        ],
        commit_every=1000,
    )

    logger.info("success insert dim_seller")

    return len(df)


def load_fact(parquet_path: str) -> int:

    logger.info("Loading fact_order")

    df = pd.read_parquet(parquet_path)

    if df.empty:
        logger.warning("No rows found for fact_order")
        return 0

    hook = get_postgres_hook()

    hook.run("""
        CREATE TABLE IF NOT EXISTS fact_order_staging (
            order_id                  VARCHAR(50),
            order_item_id             BIGINT,
            customer_id               VARCHAR(50),
            seller_id                 VARCHAR(50),
            product_id                VARCHAR(50),
            customer_zip_code_prefix  VARCHAR(10),
            seller_zip_code_prefix    VARCHAR(10),
            order_date_key            INT,
            approved_date_key         INT,
            delivered_date_key        INT,
            price                     FLOAT,
            freight_value             FLOAT,
            payment_type              VARCHAR(50),
            payment_installments      INT,
            payment_value              FLOAT,
            review_score              FLOAT,
            order_status              VARCHAR(30)
        )
    """)

    hook.run("TRUNCATE TABLE fact_order_staging")

    hook.insert_rows(
        table="fact_order_staging",
        rows=df_to_records(df),
        target_fields=[
            "order_id",
            "order_item_id",
            "customer_id",
            "seller_id",
            "product_id",
            "customer_zip_code_prefix",
            "seller_zip_code_prefix",
            "order_date_key",
            "approved_date_key",
            "delivered_date_key",
            "price",
            "freight_value",
            "payment_type",
            "payment_installments",
            "payment_value",
            "review_score",
            "order_status",
        ],
        commit_every=2000,
    )

    hook.run("""
        INSERT INTO fact_order (
            order_id,
            order_item_id,
            customer_key,
            product_key,
            seller_key,
            customer_location_key,
            seller_location_key,
            order_date_key,
            approved_date_key,
            delivered_date_key,
            price,
            freight_value,
            payment_type,
            payment_installments,
            payment_value,
            review_score,
            order_status
        )
        SELECT
            s.order_id,
            s.order_item_id,
            c.customer_key,
            p.product_key,
            sl.seller_key,
            cl.location_key,
            sll.location_key,
            s.order_date_key,
            s.approved_date_key,
            s.delivered_date_key,
            s.price,
            s.freight_value,
            s.payment_type,
            s.payment_installments,
            s.payment_value,
            s.review_score,
            s.order_status
        FROM fact_order_staging s
        JOIN dim_customer c
            ON s.customer_id = c.customer_id
        JOIN dim_product p
            ON s.product_id = p.product_id
        JOIN dim_seller sl
            ON s.seller_id = sl.seller_id
        LEFT JOIN dim_location cl
            ON cl.zip_code_prefix = s.customer_zip_code_prefix
        LEFT JOIN dim_location sll
            ON sll.zip_code_prefix = s.seller_zip_code_prefix
        ON CONFLICT (order_id, order_item_id)
        DO UPDATE SET
            customer_location_key = EXCLUDED.customer_location_key,
            seller_location_key = EXCLUDED.seller_location_key,
            price = EXCLUDED.price,
            freight_value = EXCLUDED.freight_value,
            payment_value = EXCLUDED.payment_value,
            review_score = EXCLUDED.review_score,
            order_status = EXCLUDED.order_status
    """)

    logger.info("Successfully loaded fact_order")

    return len(df)


def main(staging_path: str):

    started = datetime.now()

    load_dimension(
        f"{staging_path}/dim_location",
        "dim_location",
        "zip_code_prefix",
    )

    load_dimension(
        f"{staging_path}/dim_time",
        "dim_time",
        "date_key",
    )

    load_dimension(
        f"{staging_path}/dim_product",
        "dim_product",
        "product_id",
    )

    load_dim_customer(
        f"{staging_path}/dim_customer"
    )

    load_dim_seller(
        f"{staging_path}/dim_seller"
    )

    load_fact(
        f"{staging_path}/fact_order"
    )

    logger.info(
        f"All tables loaded "
        f"(elapsed: {datetime.now() - started})"
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--staging-path",
        required=True,
    )

    args = parser.parse_args()

    main(args.staging_path)

