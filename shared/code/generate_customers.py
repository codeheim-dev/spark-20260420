from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from faker import Faker
import random

OUTPUT_PATH = "/opt/spark/shared/data/customers"
NUM_RECORDS = 1_000_000

DOMAINS = [
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "icloud.com",
    "protonmail.com",
    "mail.com",
    "zoho.com",
    "gmx.com",
    "yandex.com",
    "aol.com",
    "live.com",
    "msn.com",
    "me.com",
    "inbox.com",
    "fastmail.com",
    "tutanota.com",
    "hey.com",
    "pm.me",
    "duck.com",
]


def generate_records():
    fake = Faker()
    rows = []
    for i in range(1, NUM_RECORDS + 1):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name}.{last_name}@{random.choice(DOMAINS)}"
        rows.append(
            (
                i,
                first_name,
                last_name,
                email,
                random.randint(100, 90000),
                fake.city(),
                fake.country_code(representation="alpha-2"),
            )
        )
    return rows


def main():
    spark = SparkSession.builder.appName("GenerateCustomers").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("firstName", StringType(), False),
            StructField("lastName", StringType(), False),
            StructField("email", StringType(), False),
            StructField("points", IntegerType(), False),
            StructField("city", StringType(), False),
            StructField("country", StringType(), False),
        ]
    )

    rows = generate_records()
    df = spark.createDataFrame(rows, schema=schema)

    (df.write.mode("overwrite").parquet(OUTPUT_PATH))

    print(f"Zapisano {df.count()} rekordów do {OUTPUT_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
