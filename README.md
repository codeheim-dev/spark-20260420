# spark-20260420

Link do danych. Skopiować do /shared/code/db
https://drive.google.com/drive/folders/1So4iwDIt7_hw6aErLuTY2kGTA0RwWY-W?usp=sharing

Categories: columns=["id", "name", "description"]

Countries: columns=["id", "name", "sigThreeChars", "baseCurrency"]

Customers: columns=["id", "firstName", "lastName", "email", "country"]

Opinions: 
```
{
    "id": np.arange(1, n + 1, dtype=np.int32),
    "productId": product_ids,
    "customerId": customer_ids,
    "rating": ratings,
    "title": np.array(TITLES, dtype=object)[title_idx],
    "reviewDate": review_dates,
    "isVerifiedPurchase": verified,
    "helpfulVotes": helpful_votes,
}
```

order_details: 
```
{
    "orderID": order_ids,
    "lineNumber": line_numbers,
    "product": products,
    "quantity": quantities,
}
```

orders:
```
{
    "id": range(1, n + 1),
    "client": [random.randint(1, NUM_CUSTOMERS) for _ in range(n)],
    "orderDate": order_dates,
    "status": [random.choice(ORDER_STATUSES) for _ in range(n)],
    "paymentStatus": [random.choice(PAYMENT_STATUSES) for _ in range(n)],
    "paymentMethod": [random.choice(PAYMENT_METHODS) for _ in range(n)],
}
```

orders: columns=["ID", "name", "categoryId", "lastUpdated", "unitPrice"]

suppliers:
```
columns=[
    "id",
    "name",
    "country",
    "contactEmail",
    "phone",
    "rating",
    "isActive",
    "since",
],
```
