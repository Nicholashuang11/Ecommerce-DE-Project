import logging
import os
import shutil
import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("silver_ingest")

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SilverIngest")
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic"
        )
        .getOrCreate()
    )

def delete_partition(partition_path):
    if os.path.exists(partition_path):
        shutil.rmtree(partition_path)


def read_parquet(
        spark: SparkSession,
        data_path: str,
        entity: str,
        ingest_date: str
):
    parquet_path = os.path.join(data_path, entity, f"batch_ingest_date={ingest_date}")
    df = spark.read.parquet(parquet_path)
    return df


def write_silver(df: DataFrame, output_path: str, entity: str, ingest_date: str):
    output_dir = os.path.join(output_path, entity)
    partition_path = os.path.join(output_dir, f"batch_ingest_date={ingest_date}")
    delete_partition(partition_path)
    (
        df.withColumn("processed_at", F.current_timestamp())
          .withColumn("batch_ingest_date", F.lit(ingest_date))
          .write
          .mode("overwrite")
          .partitionBy("batch_ingest_date")
          .parquet(output_dir)
    )
    logger.info(f"completed {entity} {partition_path}")


def transform_geolocation(geolocation: DataFrame):
    logger.info("Transform geolocation")
    return (
        geolocation
        .select(
            F.col("geolocation_zip_code_prefix").cast("string").alias("zip_code_prefix"),
            F.col("geolocation_lat").cast(FloatType()),
            F.col("geolocation_lng").cast(FloatType()),
            F.col("geolocation_city").cast("string").alias("city"),
            F.col("geolocation_state").cast("string").alias("state")
        ).dropDuplicates()
    )


def transform_category_translation(category_translation: DataFrame):
    logger.info("Transform category translation")
    return (
        category_translation.select(
            F.col("product_category_name").cast("string"),
            F.col("product_category_name_english").cast("string")
        ).dropDuplicates()
    )


def transform_customer(customer: DataFrame):
    logger.info("Transform customer")
    return (
        customer.select(
            F.col("customer_id").cast("string"),
            F.col("customer_unique_id").cast("string"),
            F.col("customer_zip_code_prefix").cast("string"),
            F.col("customer_city").cast("string"),
            F.col("customer_state").cast("string")
        )
    )


def transform_order_item(order_item: DataFrame):
    logger.info("Transform order_item")
    return (
        order_item.select(
            F.col("order_id").cast("string"),
            F.col("order_item_id").cast("string"),
            F.col("product_id").cast("string"),
            F.col("seller_id").cast("string"),
            F.col("shipping_limit_date").cast("timestamp"),
            F.col("price").cast(FloatType()),
            F.col("freight_value").cast(FloatType())
        )
    )


def transform_order_payment(order_payment: DataFrame):
    logger.info("Transform order_payment")
    return (
        order_payment.select(
            F.col("order_id").cast("string"),
            F.col("payment_sequential").cast("int"),
            F.col("payment_type").cast("string"),
            F.col("payment_installments").cast("int"),
            F.col("payment_value").cast("double"),
        )
    )


def transform_order_review(order_review: DataFrame):
    logger.info("Transform order_review")
    return (
        order_review.select(
            F.col("review_id").cast("string"),
            F.col("order_id").cast("string"),
            F.col("review_score").cast("int"),
            F.col("review_comment_title").cast("string"),
            F.col("review_comment_message").cast("string"),
            F.col("review_creation_date").cast("timestamp"),
            F.col("review_answer_timestamp").cast("timestamp"),
        )
    )


def transform_order(order: DataFrame):
    logger.info("Transform order")
    return (
        order.select(
            F.col("order_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.col("order_status").cast("string"),
            F.col("order_purchase_timestamp").cast("timestamp"),
            F.col("order_approved_at").cast("timestamp"),
            F.col("order_delivered_carrier_date").cast("timestamp"),
            F.col("order_delivered_customer_date").cast("timestamp"),
            F.col("order_estimated_delivery_date").cast("timestamp"),
        )
    )


def transform_product(product: DataFrame):
    logger.info("Transform product")
    return (
        product.select(
            F.col("product_id").cast("string"),
            F.col("product_category_name").cast("string"),
            F.col("product_name_lenght").cast("int"),
            F.col("product_description_lenght").cast("int").alias("product_description_length"),
            F.col("product_photos_qty").cast("int"),
            F.col("product_weight_g").cast("double"),
            F.col("product_length_cm").cast("double"),
            F.col("product_height_cm").cast("double"),
            F.col("product_width_cm").cast("double"),
        )
    )


def transform_seller(seller: DataFrame):
    logger.info("Transform seller")
    return (
        seller.select(
            F.col("seller_id").cast("string"),
            F.col("seller_zip_code_prefix").cast("string"),
            F.col("seller_city").cast("string"),
            F.col("seller_state").cast("string"),
        )
    )


ENTITY_TRANSFORMS = {
    "customer": transform_customer,
    "geolocation": transform_geolocation,
    "order_item": transform_order_item,
    "order_payment": transform_order_payment,
    "order_review": transform_order_review,
    "order": transform_order,
    "product": transform_product,
    "seller": transform_seller,
    "category_translation": transform_category_translation,
}
def main(data_path:str,output_path:str, ingest_date:str,only:str = None):
    spark = get_spark()
    if only:
        entities= [e.strip()for e in only.split(",") if e.strip() in ENTITY_TRANSFORMS]
        if not entities:
            logger.warning(f"entities {only} not found")
    else:
        entities = list(ENTITY_TRANSFORMS.keys())
    for entity in entities:
        transform_func = ENTITY_TRANSFORMS[entity]
        try:
            bronze_df = read_parquet(spark,data_path,entity,ingest_date)
            silver_df= transform_func(bronze_df)
            write_silver(silver_df,output_path,entity,ingest_date)
            logger.info(f"Complete ingest {entity}")
        except Exception as e:
            logger.error(f"failed processing {entity}: {e}")
            raise
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data-path",
        required=True,
    )

    parser.add_argument(
        "--output-path",
        required=True,
    )

    parser.add_argument(
        "--ingest-date",
        required=True,
    )

    parser.add_argument(
        "--only",
        required=False,
    )
    args = parser.parse_args()
    main(
        data_path=args.data_path,
        output_path=args.output_path,
        ingest_date=args.ingest_date,
        only=args.only,
    )