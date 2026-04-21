from pyspark.sql import SparkSession
from pyspark.sql.types import *

DATA_DIR = "/opt/spark/shared/code/files"


def main():
    with SparkSession.builder.appName("ReadDataFromFiles").getOrCreate() as spark:
        print("=============================================")
        df = spark.read.parquet(f"{DATA_DIR}/products.parquet")
        df.printSchema()

        print("=============================================")
        df = spark.read.csv(f"{DATA_DIR}/orders.csv", header=True)
        df.printSchema()

        print("=============================================")
        df = spark.read.parquet(f"/opt/spark/shared/data/customers")
        df.printSchema()

        print("=============================================")
        df = spark.read.csv(f"{DATA_DIR}/orders.csv", header=True, inferSchema=True)
        df.printSchema()


if __name__ == "__main__":
    main()
