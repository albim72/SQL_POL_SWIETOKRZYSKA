import pandas as pd
import json


def main():
    # 1. Extract
    customers = pd.read_csv("customers.csv")
    orders = pd.read_csv("orders.csv")
    items = pd.read_csv("order_items.csv")

    # 2. Transform - walidacja i wzbogacenie danych
    orders = orders.dropna(subset=["order_id", "customer_id"])
    items = items.dropna(subset=["order_id"])
    items = items[(items["quantity"] > 0) & (items["unit_price"] > 0)]
    items["line_total"] = items["quantity"] * items["unit_price"]

    # 3. Łączenie danych do modelu dokumentowego
    documents = []

    for _, order in orders.iterrows():
        customer_rows = customers[customers["customer_id"] == order["customer_id"]]
        if customer_rows.empty:
            print(f"Pominięto order_id={order['order_id']} - brak klienta")
            continue

        customer = customer_rows.iloc[0]
        order_items = items[items["order_id"] == order["order_id"]]

        doc = {
            "order_id": int(order["order_id"]),
            "order_date": str(order["order_date"]),
            "status": str(order["status"]),
            "customer": {
                "customer_id": int(customer["customer_id"]),
                "name": str(customer["name"]),
                "email": str(customer["email"]),
                "city": str(customer["city"]),
                "country": str(customer["country"]),
            },
            "items": [],
            "order_total": float(order_items["line_total"].sum())
        }

        for _, item in order_items.iterrows():
            doc["items"].append({
                "product": str(item["product"]),
                "category": str(item["category"]),
                "quantity": int(item["quantity"]),
                "unit_price": float(item["unit_price"]),
                "line_total": float(item["line_total"]),
            })

        documents.append(doc)

    # 4. Load do pliku JSON
    with open("orders_nosql.json", "w", encoding="utf-8") as f:
        json.dump(documents, f, indent=2, ensure_ascii=False)

    print("Zapisano plik orders_nosql.json")
    print(f"Liczba dokumentów: {len(documents)}")


if __name__ == "__main__":
    main()
