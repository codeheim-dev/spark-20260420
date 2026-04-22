from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark import StorageLevel
from decimal import Decimal

OUTPUT_DIR = "/opt/spark/shared/data"


def main():
    with SparkSession.builder.appName("ExampleSQL").getOrCreate() as spark:
        customers = spark.read.parquet("/opt/spark/shared/code/db/customers.parquet")
        orders = spark.read.parquet(
            "/opt/spark/shared/code/db/orders.parquet"
        ).withColumnRenamed("ID", "oID")
        order_details = spark.read.parquet(
            "/opt/spark/shared/code/db/order_details.parquet"
        )
        products = spark.read.parquet(
            "/opt/spark/shared/code/db/products.parquet"
        ).withColumnRenamed("ID", "productID")

        result = customers.join(orders, customers.id == orders.client, "inner")

        result = result.join(
            order_details, order_details.orderID == result.oID, "inner"
        )

        result = result.join(products, products.productID == result.product)

        result = result.filter(
            (f.col("status") == "delivered")
            & (f.col("paymentMethod").isin(["paypal", "debit_card"]))
            & (f.col("country") == "US")
        )

        # result.cache() # InMemoryTableScan ...
        result.persist(StorageLevel.DISK_ONLY)

        """
        persist
        cache == persist(StorageLevel.MEMORY_ONLY)

        DISK_ONLY
        DISK_ONLY_2
        DISK_ONLY_3
        MEMORY_AND_DISK
        MEMORY_AND_DISK_2
        MEMORY_AND_DISK_DESER
        MEMORY_ONLY
        MEMORY_ONLY_2
        NONE
        OFF_HEAP
        """

        result.groupBy("product").agg(
            f.sum(f.col("quantity")).alias("totalSold"),
            f.round(f.sum(f.col("unitPrice") * f.col("quantity")), 2).alias(
                "totalIncome"
            ),
        ).orderBy(f.col("totalIncome").desc()).show(20)

        # result.explain(extended=True)

        result.groupBy("oID").agg(
            f.sum(f.col("quantity")).alias("totalSold"),
            f.round(f.sum(f.col("unitPrice") * f.col("quantity")), 2).alias(
                "totalIncome"
            ),
        ).orderBy(f.col("totalIncome").desc()).show(20)

        result.groupBy("client").agg(
            f.round(f.sum(f.col("unitPrice") * f.col("quantity")), 2).alias(
                "totalIncome"
            )
        ).orderBy(f.col("totalIncome").desc()).show(20)

        result.groupBy("paymentMethod").agg(
            f.round(f.sum(f.col("unitPrice") * f.col("quantity")), 2).alias(
                "totalIncome"
            )
        ).orderBy(f.col("totalIncome").desc()).show(20)

        result.groupBy("categoryId").agg(
            f.round(f.sum(f.col("unitPrice") * f.col("quantity")), 2).alias(
                "totalIncome"
            )
        ).orderBy(f.col("totalIncome").desc()).show(20)

        result.groupBy("categoryId").agg(
            f.round(f.sum(f.col("unitPrice") * f.col("quantity")), 2).alias(
                "totalIncome"
            )
        ).orderBy(f.col("totalIncome").desc()).show(20)

        result.groupBy("categoryId").agg(
            f.round(f.sum(f.col("unitPrice") * f.col("quantity")), 2).alias(
                "totalIncome"
            )
        ).orderBy(f.col("totalIncome").desc()).show(20)


if __name__ == "__main__":
    main()
