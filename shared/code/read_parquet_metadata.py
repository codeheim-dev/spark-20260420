import pandas as pd

INPUT_PATH = "shared/data/products.parquet"

df = pd.read_parquet(INPUT_PATH)

print("=== Schema (dtypes) ===")
print(df.dtypes)

print(f"\n=== Liczba rekordów: {len(df):,} ===")

print("\n=== Describe (statystyki) ===")
print(df.describe(include="all"))
