from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from decimal import Decimal

OUTPUT_DIR = "/opt/spark/shared/data"


def main():
    with SparkSession.builder.appName("Joins").getOrCreate() as spark:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

        # Scenariusz 01
        customers = spark.read.parquet("/opt/spark/shared/code/db/customers.parquet")
        orders = spark.read.parquet("/opt/spark/shared/code/db/orders.parquet")

        result = customers.join(
            orders,
            orders.client == customers.id,
            "inner"
        )

        result.explain(extended=True)
        result.show(15)


if __name__ == "__main__":
    main()
