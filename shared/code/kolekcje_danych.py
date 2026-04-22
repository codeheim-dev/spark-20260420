from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark import StorageLevel
from decimal import Decimal

OUTPUT_DIR = "/opt/spark/shared/data"


def main():
    with SparkSession.builder.appName("KolekcjeDanych").getOrCreate() as spark:
        spark.sparkContext.setLogLevel("WARN")
        order_details = spark.read.parquet(
            "/opt/spark/shared/code/db/order_details.parquet"
        )

        df = order_details.groupBy("product").agg(
            f.sort_array(f.collect_set(f.col("orderID")), asc=False).alias("orders")
        )

        # df.select(f.col("orders")[0], f.col("product")).show(20, truncate=False)

        orders = spark.read.parquet("/opt/spark/shared/code/db/orders.parquet")

        df = orders.select(
            f.col("id"),
            f.struct(
                ["client", "orderDate", "status", "paymentStatus", "paymentMethod"]
            ).alias("body"),
        )

        df.select(f.col("body.status")).show()


if __name__ == "__main__":
    main()
