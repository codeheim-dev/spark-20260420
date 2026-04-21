from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from decimal import Decimal

OUTPUT_DIR = "/opt/spark/shared/data"


def main():
    with SparkSession.builder.appName("Partitions-003").getOrCreate() as spark:

        # df = spark.read.parquet('/opt/spark/shared/code/files/products2.parquet')
        # df.write.mode('overwrite').partitionBy('category').parquet('/opt/spark/shared/data/products2')

        # predicte pushdown
        df = spark.read.parquet("/opt/spark/shared/data/products2").filter(
            f.col("category") == "Automotive"
        )
        print(df.count(), df.rdd.getNumPartitions())

        # Brak możliwości predykcji
        df = spark.read.parquet("/opt/spark/shared/code/files/products2.parquet")
        df = df.filter(f.col("category") == "Automotive")
        print(df.count(), df.rdd.getNumPartitions())


if __name__ == "__main__":
    main()
