import os
import sys
#os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot"
#os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
import argparse
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F 
from pyspark.sql.types import FloatType, IntegerType
import psycopg2
conn = psycopg2.connect("dbname=ecommerce_dw user=commerce password=commerce host=localhost port=5475")
conn.autocommit = True
cursor = conn.cursor()
with open('Createtable.sql','r')as f:
    sql_commands=f.read()
    cursor.execute(sql_commands)
conn.commit()
cursor.close()
conn.close()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("transform_dims")
def get_spark(app_name: str = "OlistDims")-> SparkSession:
    return(
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy","LEGACY")
        .getOrCreate()
    )
def read_csv(spark: SparkSession, path: str)-> DataFrame:
    logger.info(f"Reading file: {path}")
    return spark.read.csv(path,header=True, inferSchema=True)

def to_date_key(col_name: str, alias: str):
    return (
        F.date_format(F.to_date(F.col(col_name)), "yyyyMMdd")
         .cast(IntegerType())
         .alias(alias)
)
def transform_fact_order(orders: DataFrame, order_items: DataFrame, payments: DataFrame, reviews:DataFrame)->DataFrame:
    logger.info("Transform fact_order")
    payments_agg = (
        payments
        .groupby("order_id")
        .agg(
            F.mode("payment_type").alias("payment_type"),
            F.first(
                F.col("payment_installments").cast(IntegerType())
            ).alias("payment_installments"),
            F.round(
                F.sum(F.col("payment_value").cast(FloatType())),2).alias("payment_value")
        )
    )
    reviews_agg = (
        reviews.groupby("order_id")
        .agg(
            F.round(F.avg("review_score"),1)
            .cast(FloatType())
            .alias("review_score")
        )
    )
    orders_clean = (
        orders
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("order_status"),
            to_date_key("order_purchase_timestamp",      "order_date_key"),
            to_date_key("order_approved_at",             "approved_date_key"),
            to_date_key("order_delivered_customer_date", "delivered_date_key"),
        )
    )
    
    return (
        orders_clean.join(order_items,on="order_id",how="inner")
        .join(payments_agg,on="order_id",how="left")
        .join(reviews_agg,on="order_id",how="left")
        .select(
            F.col("order_id"),
            F.col("order_item_id").cast("bigint"),
            F.col("customer_id"),
            F.col("seller_id"),
            F.col("product_id"),
            F.col("order_date_key"),
            F.col("approved_date_key"),
            F.col("delivered_date_key"),
            F.col("price"),
            F.col("freight_value"),
            F.col("payment_type"),
            F.col("payment_installments"),
            F.col("payment_value").cast(FloatType()),
            F.col("review_score"),
            F.col("order_status")
        )
        .dropna(subset=["order_id","customer_id","product_id","seller_id"])
        .filter(F.col("price")>=0)
        .filter(F.col("freight_value")>=0)

)


def main(data_path: str, output_path: str):
    spark = get_spark()

    orders      = read_csv(spark, f"{data_path}/olist_orders_dataset.csv")
    order_items = read_csv(spark, f"{data_path}/olist_order_items_dataset.csv")
    payments    = read_csv(spark, f"{data_path}/olist_order_payments_dataset.csv")
    reviews     = read_csv(spark, f"{data_path}/olist_order_reviews_dataset.csv")

    fact = transform_fact_order(orders, order_items, payments, reviews)
    fact.write.mode("overwrite").parquet(f"{output_path}/fact_order")
    count = fact.count()
    logger.info(f"fact_order rows: {count}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path",   required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()
    main(args.data_path, args.output_path)
