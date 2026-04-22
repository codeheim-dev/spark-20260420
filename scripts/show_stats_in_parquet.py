import pandas as pd

INPUT_PATH = ["/home/teamsharq/spark-20260420/shared/code/files/products.parquet"]

for file in INPUT_PATH:

    df = pd.read_parquet(file)

    print("=== Schema (dtypes) ===")
    print(df.dtypes)

    print(f"\n=== Liczba rekordów: {len(df):,} ===")

    print("\n=== Describe (statystyki) ===")
    print(df.describe(include="all"))
