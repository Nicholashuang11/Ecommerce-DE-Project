import argparse
import logging
import os
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import shutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger("bronze_ingest")



ENTITY_TO_FILE = {
    "customer": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_item": "olist_order_items_dataset.csv",
    "order_payment": "olist_order_payments_dataset.csv",
    "order_review": "olist_order_reviews_dataset.csv",
    "order": "olist_orders_dataset.csv",
    "product": "olist_products_dataset.csv",
    "seller": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BronzeIngest")
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic",
        )
        .getOrCreate()
    )
def delete_partition_if_exists(partition_path: str):
    if os.path.exists(partition_path):
        logger.info(f"deleting existing partition: {partition_path}")
        shutil.rmtree(partition_path)
    

def ingest_file(
    spark: SparkSession,
    data_path: str,
    output_path: str,
    entity: str,
    ingest_date: str,
):
    filename = ENTITY_TO_FILE[entity]
    csv_path = os.path.join(data_path,entity,f"ingestion_date={ingest_date}")
    if not os.path.exists(csv_path):
        logger.warning(f"no file found for {entity} at {csv_path}")
        return
    logger.info(f"ingesting {filename} from {csv_path}")    
    output_dir = os.path.join(output_path,entity)
    partition_path = os.path.join(output_dir,f"batch_ingest_date={ingest_date}")
    delete_partition_if_exists(partition_path)
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(csv_path)
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("batch_ingest_date", F.lit(ingest_date))
    )


    (
        df.write
        .mode("overwrite")
        .partitionBy("batch_ingest_date")
        .parquet(output_dir)
    )

    logger.info(
        f"completed '{entity}' "
        f"-> {output_dir}/ingest_date={ingest_date}"
    )


def main(
    data_path: str,
    output_path: str,
    ingest_date: str,
    only: Optional[str] = None,
):
    if only and only not in ENTITY_TO_FILE:
        raise ValueError(
            f"Unknown entity '{only}'. "
            f"Valid entities: {', '.join(ENTITY_TO_FILE)}"
        )

    spark = get_spark()

    try:
        entities= [only] if only else list(ENTITY_TO_FILE.keys())
        for entity in entities:
            ingest_file(
                spark=spark,
                data_path=data_path,
                output_path=output_path,
                entity=entity,
                ingest_date=ingest_date
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest to bronze layer"
    )

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