import argparse
import logging
import os
from datetime import datetime
import pandas as pd
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("load")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "ecommerce_dw"),
    "user":     os.getenv("POSTGRES_USER",     "commerce"),
    "password": os.getenv("POSTGRES_PASSWORD", "commerce"),
}

def connectdb():
    return psycopg2.connect(**DB_CONFIG)
import numpy as np

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


def load_dimension(parquet_path: str, table: str, conflict_col: str) -> int:
    logger.info(f"Loading {table}")
    df = pd.read_parquet(parquet_path)

    if df.empty:
        logger.warning(f"No rows found in table {table}")
        return 0

    cols         = list(df.columns)
    col_list     = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    update_set   = ", ".join(
        [f"{c} = EXCLUDED.{c}" for c in cols if c != conflict_col]
    )

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_col}) DO UPDATE SET {update_set}
    """

    conn = connectdb()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                sql,
                df_to_records(df),
                page_size=1000,
            )
        conn.commit()
    except Exception:
        conn.rollback() 
        raise
    finally:
        conn.close()

    logger.info(f"Done load {table}. Total rows: {len(df):,}")
    return len(df)


def load_dim_customer(parquet_path: str) -> int:
    logger.info("Loading dim_customer")
    df = pd.read_parquet(parquet_path)

    if df.empty:
        logger.warning("No rows found for dim_customer")
        return 0

    conn = connectdb()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE customer_staging (
                    customer_id        VARCHAR(50),
                    customer_unique_id VARCHAR(50),
                    zip_code_prefix    VARCHAR(10)
                ) ON COMMIT DROP
            """)
            psycopg2.extras.execute_batch(
                cur,
                "INSERT INTO customer_staging VALUES (%s, %s, %s)",
                df_to_records(df),
                page_size=1000,
            )
            cur.execute("""
                INSERT INTO dim_customer (
                    customer_id, customer_unique_id,
                    zip_code_prefix, location_key
                )
                SELECT
                    cs.customer_id,
                    cs.customer_unique_id,
                    cs.zip_code_prefix,
                    l.location_key
                FROM customer_staging cs
                LEFT JOIN dim_location l ON cs.zip_code_prefix = l.zip_code_prefix
                ON CONFLICT (customer_id) DO UPDATE SET
                    customer_unique_id = EXCLUDED.customer_unique_id,
                    zip_code_prefix    = EXCLUDED.zip_code_prefix,
                    location_key       = EXCLUDED.location_key;
            """)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("success insert dim_customer")
    return len(df)


def load_dim_seller(parquet_path: str) -> int:
    logger.info("Load dim_seller")
    df = pd.read_parquet(parquet_path)

    if df.empty:
        logger.warning("No row found for dim_seller")
        return 0

    conn = connectdb()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE seller_staging (
                    seller_id       VARCHAR(50),
                    zip_code_prefix VARCHAR(10)
                ) ON COMMIT DROP;
            """)
            psycopg2.extras.execute_batch(
                cur,
                "INSERT INTO seller_staging VALUES (%s, %s)",
                df_to_records(df),
            )
            cur.execute("""
                INSERT INTO dim_seller (seller_id, zip_code_prefix, location_key)
                SELECT ss.seller_id, ss.zip_code_prefix, l.location_key
                FROM seller_staging ss
                LEFT JOIN dim_location l ON l.zip_code_prefix = ss.zip_code_prefix
                ON CONFLICT (seller_id) DO UPDATE SET
                    zip_code_prefix = EXCLUDED.zip_code_prefix,
                    location_key    = EXCLUDED.location_key;
            """)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("success insert dim_seller")
    return len(df)


def load_fact(parquet_path: str) -> int:
    logger.info("Loading fact_order")
    df = pd.read_parquet(parquet_path)

    if df.empty:
        logger.warning("No rows found for fact_order") 
        return 0

    conn = connectdb()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE fact_order_staging (
                    order_id             VARCHAR(50),
                    order_item_id        BIGINT,
                    customer_id          VARCHAR(50),
                    seller_id            VARCHAR(50),
                    product_id           VARCHAR(50),
                    order_date_key       INT,
                    approved_date_key    INT,
                    delivered_date_key   INT,
                    price                FLOAT,
                    freight_value        FLOAT,
                    payment_type         VARCHAR(50),
                    payment_installments INT,
                    payment_value        FLOAT,
                    review_score         FLOAT,
                    order_status         VARCHAR(30)
                ) ON COMMIT DROP
            """)

            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO fact_order_staging VALUES
                    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                df_to_records(df),
                page_size=2000,
            )

       
            cur.execute("""
                INSERT INTO fact_order (
                    order_id, order_item_id,
                    customer_key, product_key, seller_key,
                    order_date_key, approved_date_key, delivered_date_key,
                    price, freight_value,
                    payment_type, payment_installments, payment_value,
                    review_score, order_status
                )
                SELECT
                    s.order_id,
                    s.order_item_id,
                    c.customer_key,
                    p.product_key,
                    sl.seller_key,
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
                JOIN dim_customer c  ON s.customer_id = c.customer_id
                JOIN dim_product  p  ON s.product_id  = p.product_id
                JOIN dim_seller   sl ON s.seller_id   = sl.seller_id
                ON CONFLICT (order_id, order_item_id) DO UPDATE SET
                    price                = EXCLUDED.price,
                    freight_value        = EXCLUDED.freight_value,
                    payment_value        = EXCLUDED.payment_value,
                    review_score         = EXCLUDED.review_score,
                    order_status         = EXCLUDED.order_status;
            """)

        conn.commit()
    except Exception:
        conn.rollback() 
        raise
    finally:
        conn.close()

    logger.info("Successfully loaded fact_order")
    return len(df)


def main(staging_path: str):
    started = datetime.now()  

    load_dimension(f"{staging_path}/dim_location", "dim_location", "zip_code_prefix")
    load_dimension(f"{staging_path}/dim_time",     "dim_time",     "date_key")
    load_dimension(f"{staging_path}/dim_product",  "dim_product",  "product_id")
    load_dim_customer(f"{staging_path}/dim_customer")
    load_dim_seller(f"{staging_path}/dim_seller")
    load_fact(f"{staging_path}/fact_order")

    logger.info(f"All tables loaded (elapsed: {datetime.now() - started})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging-path", required=True)
    args = parser.parse_args()
    main(args.staging_path)
