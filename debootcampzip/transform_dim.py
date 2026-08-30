import os
import sys
os.environ["PATH"] += r";C:\hadoop\bin"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
import argparse
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F 
from pyspark.sql.types import FloatType, IntegerType
import psycopg2

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
def transform_dim_location(geolocation: DataFrame)->DataFrame:
    logger.info("Transform dim_location")
    return(
        geolocation
        .groupby("geolocation_zip_code_prefix")
        .agg(
        F.first("geolocation_city").alias("city"),
        F.first("geolocation_state").alias("state"),
        F.round(F.avg("geolocation_lat").cast(FloatType()), 6).alias("avg_lat"),
        F.round(F.avg("geolocation_lng").cast(FloatType()),6).alias("avg_lng")
        )
        .withColumnRenamed("geolocation_zip_code_prefix","zip_code_prefix")
        .withColumn("zip_code_prefix", F.col("zip_code_prefix").cast("string"))
        .dropna(subset=["zip_code_prefix"])
    )
def transform_dim_customer(customers: DataFrame) -> DataFrame:
    logger.info("Transform dim_customer")
    return(
        customers
        .select(
            F.col("customer_id"),
            F.col("customer_unique_id"),
            F.col("customer_zip_code_prefix").cast("string").alias("zip_code_prefix")
        )
        .dropDuplicates(["customer_id"])
        .dropna(subset=["customer_id"])
    )
def transform_dim_seller(sellers: DataFrame) -> DataFrame:
    logger.info("Transform dim_seller")
    return (
        sellers
        .select(
            F.col("seller_id"),
            F.col("seller_zip_code_prefix").cast("string").alias("zip_code_prefix"),
        )
        .dropDuplicates(["seller_id"])
        .dropna(subset=["seller_id"])
    )

def transform_dim_product(products: DataFrame, translations: DataFrame) -> DataFrame:
    logger.info("Transform dim_product")
    return (
        products
        .join(translations, on="product_category_name", how="left")
        .select(
            F.col("product_id"),
            F.col("product_category_name"),
            F.col("product_category_name_english"),
            F.col("product_weight_g").cast(FloatType()),
            F.col("product_length_cm").cast(FloatType()),
            F.col("product_height_cm").cast(FloatType()),
            F.col("product_width_cm").cast(FloatType()),
        )
        .dropDuplicates(["product_id"])
        .dropna(subset=["product_id"])
        .fillna({"product_category_name_english": "unknown"})
    )

def transform_dim_time(orders: DataFrame) -> DataFrame:
    logger.info("Transform dim_time")

    all_dates = (
        orders.select(F.to_date("order_purchase_timestamp").alias("full_date"))
        .union(orders.select(F.to_date("order_approved_at").alias("full_date")))
        .union(orders.select(F.to_date("order_delivered_customer_date").alias("full_date")))
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
            "day_name", "is_weekend"
        )
        .orderBy("date_key")
    )

def main(data_path: str, output_path:str):
    spark = get_spark()
    geolocation=read_csv(spark,f"{data_path}/olist_geolocation_dataset.csv")
    customers = read_csv(spark, f"{data_path}/olist_customers_dataset.csv")
    sellers = read_csv(spark, f"{data_path}/olist_sellers_dataset.csv")
    products = read_csv(spark, f"{data_path}/olist_products_dataset.csv")
    orders  = read_csv(spark, f"{data_path}/olist_orders_dataset.csv")
    translations= read_csv(spark, f"{data_path}/product_category_name_translation.csv")

    dim_location = transform_dim_location(geolocation)
    dim_customer = transform_dim_customer(customers)
    dim_seller = transform_dim_seller(sellers) 
    dim_product = transform_dim_product(products,translations)
    dim_time = transform_dim_time(orders)

    dim_location.write.mode("overwrite").parquet(f"{output_path}/dim_location")
    dim_customer.write.mode("overwrite").parquet(f"{output_path}/dim_customer")
    dim_seller.write.mode("overwrite").parquet(f"{output_path}/dim_seller")
    dim_product.write.mode("overwrite").parquet(f"{output_path}/dim_product")
    dim_time.write.mode("overwrite").parquet(f"{output_path}/dim_time")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path",   required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()
    main(args.data_path, args.output_path)
 
