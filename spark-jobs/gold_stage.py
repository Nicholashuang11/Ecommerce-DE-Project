import argparse
import logging
import os
import shutil

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("gold_stage")


def get_spark(app_name: str = "OlistGold") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def read_silver(spark: SparkSession, data_path: str, entity: str, ingest_date: str) -> DataFrame:
    parquet_path = os.path.join(data_path, entity, f"batch_ingest_date={ingest_date}")
    logger.info(f"Reading silver: {parquet_path}")
    return spark.read.parquet(parquet_path)


def write_gold(df: DataFrame, output_path: str, table_name: str):
    output_dir = os.path.join(output_path, table_name)
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    df.write.mode("overwrite").parquet(output_dir)
    logger.info(f"completed {table_name} -> {output_dir}")


def transform_dim_location(geolocation: DataFrame) -> DataFrame:
    logger.info("Transform dim_location")
    return (
        geolocation
        .groupby("zip_code_prefix")
        .agg(
            F.first("city").alias("city"),
            F.first("state").alias("state"),
            F.round(F.avg("geolocation_lat"), 6).alias("avg_lat"),
            F.round(F.avg("geolocation_lng"), 6).alias("avg_lng"),
        )
        .dropna(subset=["zip_code_prefix"])
    )


def transform_dim_customer(customer: DataFrame) -> DataFrame:
    logger.info("Transform dim_customer")
    return (
        customer
        .select(
            F.col("customer_id"),
            F.col("customer_unique_id"),
            F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        )
        .dropDuplicates(["customer_id"])
        .dropna(subset=["customer_id"])
    )


def transform_dim_seller(seller: DataFrame) -> DataFrame:
    logger.info("Transform dim_seller")
    return (
        seller
        .select(
            F.col("seller_id"),
            F.col("seller_zip_code_prefix").alias("zip_code_prefix"),
        )
        .dropDuplicates(["seller_id"])
        .dropna(subset=["seller_id"])
    )


def transform_dim_product(product: DataFrame, category_translation: DataFrame) -> DataFrame:
    logger.info("Transform dim_product")
    return (
        product
        .join(category_translation, on="product_category_name", how="left")
        .select(
            F.col("product_id"),
            F.col("product_category_name"),
            F.col("product_category_name_english"),
            F.col("product_weight_g"),
            F.col("product_length_cm"),
            F.col("product_height_cm"),
            F.col("product_width_cm"),
        )
        .dropDuplicates(["product_id"])
        .dropna(subset=["product_id"])
        .fillna({"product_category_name_english": "unknown"})
    )


def transform_dim_time(order: DataFrame) -> DataFrame:
    logger.info("Transform dim_time")
    all_dates = (
        order.select(F.to_date("order_purchase_timestamp").alias("full_date"))
        .union(order.select(F.to_date("order_approved_at").alias("full_date")))
        .union(order.select(F.to_date("order_delivered_customer_date").alias("full_date")))
        .dropna()
        .dropDuplicates(["full_date"])
    )
    return (
        all_dates
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
        .select(
            "date_key", "full_date", "year", "quarter", "month",
            "month_name", "week_of_year", "day_of_month", "day_of_week",
            "day_name", "is_weekend",
        )
        .orderBy("date_key")
    )


def to_date_key(col_name: str, alias: str):
    return (
        F.date_format(F.to_date(F.col(col_name)), "yyyyMMdd")
         .cast(IntegerType())
         .alias(alias)
    )


def transform_fact_order(
    order: DataFrame,
    order_item: DataFrame,
    order_payment: DataFrame,
    order_review: DataFrame,
    customer: DataFrame,
    seller: DataFrame,
) -> DataFrame:
    logger.info("Transform fact_order")

    payments_agg = (
        order_payment
        .groupby("order_id")
        .agg(
            F.mode("payment_type").alias("payment_type"),
            F.first("payment_installments").alias("payment_installments"),
            F.round(F.sum("payment_value"), 2).alias("payment_value"),
        )
    )
    reviews_agg = (
        order_review
        .groupby("order_id")
        .agg(
            F.round(F.avg("review_score"), 1).cast(FloatType()).alias("review_score")
        )
    )

    # pull zip prefixes directly onto the fact grain so we can resolve
    # customer_location_key / seller_location_key against dim_location at load time
    customer_zip = customer.select(
        F.col("customer_id"),
        F.col("customer_zip_code_prefix"),
    )
    seller_zip = seller.select(
        F.col("seller_id"),
        F.col("seller_zip_code_prefix"),
    )

    orders_clean = (
        order
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("order_status"),
            to_date_key("order_purchase_timestamp", "order_date_key"),
            to_date_key("order_approved_at", "approved_date_key"),
            to_date_key("order_delivered_customer_date", "delivered_date_key"),
        )
        .join(customer_zip, on="customer_id", how="left")
    )

    return (
        orders_clean
        .join(order_item, on="order_id", how="inner")
        .join(seller_zip, on="seller_id", how="left")
        .join(payments_agg, on="order_id", how="left")
        .join(reviews_agg, on="order_id", how="left")
        .select(
            F.col("order_id"),
            F.col("order_item_id").cast("bigint"),
            F.col("customer_id"),
            F.col("seller_id"),
            F.col("product_id"),
            F.col("customer_zip_code_prefix"),
            F.col("seller_zip_code_prefix"),
            F.col("order_date_key"),
            F.col("approved_date_key"),
            F.col("delivered_date_key"),
            F.col("price"),
            F.col("freight_value"),
            F.col("payment_type"),
            F.col("payment_installments"),
            F.col("payment_value"),
            F.col("review_score"),
            F.col("order_status"),
        )
        .dropna(subset=["order_id", "customer_id", "product_id", "seller_id"])
        .filter(F.col("price") >= 0)
        .filter(F.col("freight_value") >= 0)
    )

def main(data_path: str, output_path: str, ingest_date: str, only: str = None):
    spark = get_spark()

    geolocation = read_silver(spark, data_path, "geolocation", ingest_date)
    customer = read_silver(spark, data_path, "customer", ingest_date)
    seller = read_silver(spark, data_path, "seller", ingest_date)
    product = read_silver(spark, data_path, "product", ingest_date)
    category_translation = read_silver(spark, data_path, "category_translation", ingest_date)
    order = read_silver(spark, data_path, "order", ingest_date)
    order_item = read_silver(spark, data_path, "order_item", ingest_date)
    order_payment = read_silver(spark, data_path, "order_payment", ingest_date)
    order_review = read_silver(spark, data_path, "order_review", ingest_date)

    GOLD_TABLES = {
        "dim_location": lambda: transform_dim_location(geolocation),
        "dim_customer": lambda: transform_dim_customer(customer),
        "dim_seller": lambda: transform_dim_seller(seller),
        "dim_product": lambda: transform_dim_product(product, category_translation),
        "dim_time": lambda: transform_dim_time(order),
        "fact_order": lambda: transform_fact_order(
            order, order_item, order_payment, order_review, customer, seller
        ),
    }

    if only:
        tables = [t.strip() for t in only.split(",") if t.strip() in GOLD_TABLES]
        if not tables:
            logger.warning(f"tables {only} not found")
    else:
        tables = list(GOLD_TABLES.keys())

    for table_name in tables:
        try:
            df = GOLD_TABLES[table_name]()
            write_gold(df, output_path, table_name)
            logger.info(f"Complete gold {table_name}")
        except Exception as e:
            logger.error(f"failed processing {table_name}: {e}")
            raise

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--ingest-date", required=True)
    parser.add_argument("--only", required=False)
    args = parser.parse_args()
    main(
        data_path=args.data_path,
        output_path=args.output_path,
        ingest_date=args.ingest_date,
        only=args.only,
    )