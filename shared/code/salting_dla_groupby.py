from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark import StorageLevel
from decimal import Decimal

OUTPUT_DIR = "/opt/spark/shared/data"


def main():
    with SparkSession.builder.appName("SaltingGroupBy").getOrCreate() as spark:
        order_details = spark.read.parquet(
            "/opt/spark/shared/code/db/order_details.parquet"
        )

        order_details = order_details.withColumn("salt", (f.rand() * 2).cast("int"))

        # Agregacja czesciowa

        df = order_details.groupBy("product", "salt").agg(
            f.sum(f.col("quantity")).alias("partial_sum")
        )

        # Druga agregacja

        df.groupBy("product").agg(f.sum(f.col("partial_sum")).alias("finalsum")).show()


if __name__ == "__main__":
    main()
