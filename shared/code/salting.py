from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from math import floor
from random import randint
from decimal import Decimal

OUTPUT_DIR = "/opt/spark/shared/data"


def main():
    with SparkSession.builder.appName("Salting").getOrCreate() as spark:
        SALT = 2

        # Dodaj salt do problematycznego DF
        df = spark.read.parquet("/opt/spark/shared/code/files/products_massive.parquet")
        df.withColumn("salt", f.floor(f.rand() * SALT))

        values = [floor(randint() * 2) for _ in range(2)]

        salt_df = spark.createDataFrame(values, ["salt_key"])

        df.join(salt_df, "df.salt = salt_df.salt_key").drop("salt", "salt_key")
