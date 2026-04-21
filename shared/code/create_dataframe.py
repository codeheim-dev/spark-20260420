from pyspark.sql import SparkSession
from pyspark.sql.types import *

OUTPUT_DIR = "/opt/spark/shared/data"


def main():
    with SparkSession.builder.appName("CreateDataFrame").getOrCreate() as spark:
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("person", StringType(), False),
                StructField("points", LongType(), False),
                StructField("average_rank", DecimalType(3, 1), False),
            ]
        )

        data = [
            ("3", 12000, "Jan Kowalski", 7.2),
            (1, "Adam Nowak", 4500, 4.5),
            (2, "Jan Kowalski", 6000, 6.8),
        ]

        columns = ["id", "person", "points", "average_rank"]

        df = spark.createDataFrame(data=data, schema=schema)
        df.show()
        print(f"Kolumny DF: {df.columns}")
        df.printSchema()


if __name__ == "__main__":
    main()
