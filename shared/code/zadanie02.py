from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from decimal import Decimal

OUTPUT_DIR = "/opt/spark/shared/data"


def main():
    with SparkSession.builder.appName("Zadanie02").getOrCreate() as spark:
        df = spark.read.parquet("/opt/spark/shared/code/files/products.parquet")
        df.show()


if __name__ == "__main__":
    main()
