from pyspark.sql import SparkSession
from pyspark.sql.types import *
from decimal import Decimal

OUTPUT_DIR = "/opt/spark/shared/data"


def main():
    with SparkSession.builder.appName("RepartitioningDF").getOrCreate() as spark:
        spark.sparkContext.setLogLevel("WARN")

        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("person", StringType(), False),
                StructField("points", LongType(), False),
                StructField("average_rank", DecimalType(3, 1), False),
            ]
        )

        data = [
            (1, "Adam Nowak", 4500, Decimal("4.5")),
            (2, "Jan Kowalski", 6000, Decimal("6.8")),
        ]

        df = spark.createDataFrame(data=data, schema=schema)
        df.show()
        df.printSchema()
        print(f"Liczba partycji: {df.rdd.getNumPartitions()}")
        df.repartition(1).write.mode("overwrite").csv(
            "/opt/spark/shared/data/example", header=True
        )


if __name__ == "__main__":
    main()
