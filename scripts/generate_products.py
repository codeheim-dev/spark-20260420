import random
from datetime import date

import pandas as pd
from faker import Faker

OUTPUT_PATH = "shared/code/files/products.parquet"
NUM_RECORDS = 200_000

CATEGORIES = [
    "Electronics",
    "Clothing",
    "Books",
    "Home & Garden",
    "Sports & Outdoors",
    "Toys & Games",
    "Food & Beverages",
    "Beauty & Personal Care",
    "Automotive",
    "Health & Wellness",
    "Office Supplies",
    "Pet Supplies",
    "Music & Movies",
    "Tools & Hardware",
    "Jewelry & Watches",
]

BRANDS = [
    "TechNova",
    "UrbanStyle",
    "PageTurner",
    "GreenLeaf",
    "ProSport",
    "FunZone",
    "FreshBite",
    "GlowUp",
    "DriveMax",
    "VitalCare",
    "OfficeEdge",
    "PawPal",
    "SoundWave",
    "BuildRight",
    "LuxeGem",
    "NexGen",
    "EcoLine",
    "PeakPro",
    "SmartHome",
    "AquaFit",
    "SkyTech",
    "IronClad",
    "PurePath",
    "SwiftGear",
    "OrionBrand",
]

SUPPLIERS = [f"Supplier_{i:04d}" for i in range(1, 501)]

UPDATE_DATES = [
    date(2026, 4, 16),
    date(2026, 4, 17),
    date(2026, 4, 18),
    date(2026, 4, 19),
    date(2026, 4, 20),
]

fake = Faker()
Faker.seed(0)
random.seed(0)


def generate_data() -> pd.DataFrame:
    product_ids = range(1, NUM_RECORDS + 1)
    product_names = [
        f"{fake.word().capitalize()} {fake.word().capitalize()} {random.choice(['Pro', 'Lite', 'Max', 'Ultra', 'Plus', 'Elite', 'Basic', 'Mini', ''])}".strip()
        for _ in range(NUM_RECORDS)
    ]
    categories = [random.choice(CATEGORIES) for _ in range(NUM_RECORDS)]
    brands = [random.choice(BRANDS) for _ in range(NUM_RECORDS)]
    prices = [round(random.uniform(0.99, 9999.99), 2) for _ in range(NUM_RECORDS)]
    stock_quantities = [random.randint(0, 10_000) for _ in range(NUM_RECORDS)]
    supplier_ids = [random.choice(SUPPLIERS) for _ in range(NUM_RECORDS)]
    updated_dates = [random.choice(UPDATE_DATES) for _ in range(NUM_RECORDS)]

    return pd.DataFrame(
        {
            "product_id": product_ids,  # int – unikalny ID
            "product_name": product_names,  # string – wysoka kardynalność
            "category": categories,  # string – 15 wartości
            "brand": brands,  # string – 25 wartości
            "price": prices,  # float – zakres 0.99–9999.99
            "stock_quantity": stock_quantities,  # int – 0–10 000
            "supplier_id": supplier_ids,  # string – 500 dostawców
            "updated_date": updated_dates,  # date – tylko 2026-04-16..20
        }
    )


def main():
    print(f"Generating {NUM_RECORDS:,} product records -> {OUTPUT_PATH}")
    df = generate_data()
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"Done. File saved to: {OUTPUT_PATH}")
    print(df.dtypes)
    print(df.head(3))


if __name__ == "__main__":
    main()
