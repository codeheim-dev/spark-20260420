import random
from datetime import date

import pandas as pd
from faker import Faker

OUTPUT_PATH = "shared/code/files/orders.csv"
NUM_RECORDS = 600_000
BATCH_SIZE = 100_000

STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
CATEGORIES = [
    "Electronics",
    "Clothing",
    "Books",
    "Home & Garden",
    "Sports",
    "Toys",
    "Food",
    "Beauty",
    "Automotive",
]
PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "paypal",
    "bank_transfer",
    "cash_on_delivery",
    "crypto",
]

fake = Faker()
Faker.seed(42)
random.seed(42)


def generate_batch(start_id: int, size: int) -> pd.DataFrame:
    order_ids = range(start_id, start_id + size)
    customer_ids = [random.randint(1, 500_000) for _ in range(size)]
    order_dates = [
        fake.date_between(start_date=date(2022, 1, 1), end_date=date(2026, 4, 21))
        for _ in range(size)
    ]
    statuses = [random.choice(STATUSES) for _ in range(size)]
    categories = [random.choice(CATEGORIES) for _ in range(size)]
    product_names = [fake.bs().title()[:60] for _ in range(size)]
    quantities = [random.randint(1, 50) for _ in range(size)]
    unit_prices = [round(random.uniform(0.99, 4999.99), 2) for _ in range(size)]
    discount_pcts = [
        round(random.choice([0.0, 0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30]), 2)
        for _ in range(size)
    ]
    total_amounts = [
        round(qty * price * (1 - disc), 2)
        for qty, price, disc in zip(quantities, unit_prices, discount_pcts)
    ]
    payment_methods = [random.choice(PAYMENT_METHODS) for _ in range(size)]
    is_express = [random.choice([True, False]) for _ in range(size)]

    return pd.DataFrame(
        {
            "order_id": order_ids,
            "customer_id": customer_ids,
            "order_date": order_dates,
            "status": statuses,
            "product_category": categories,
            "product_name": product_names,
            "quantity": quantities,
            "unit_price": unit_prices,
            "discount_pct": discount_pcts,
            "total_amount": total_amounts,
            "payment_method": payment_methods,
            "is_express": is_express,
        }
    )


def main():
    print(f"Generating {NUM_RECORDS:,} order records -> {OUTPUT_PATH}")

    first_batch = True
    records_written = 0

    for batch_start in range(1, NUM_RECORDS + 1, BATCH_SIZE):
        current_size = min(BATCH_SIZE, NUM_RECORDS - batch_start + 1)
        df = generate_batch(batch_start, current_size)

        df.to_csv(
            OUTPUT_PATH,
            mode="w" if first_batch else "a",
            index=False,
            header=first_batch,
        )

        first_batch = False
        records_written += current_size

        if records_written % 500_000 == 0 or records_written == NUM_RECORDS:
            print(f"  {records_written:,} / {NUM_RECORDS:,} records written...")

    print(f"Done. File saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
