"""Szybki przykład bazy kolumnowej / wide-column w Pythonie.

Idea:
- dane grupujemy według klucza partycjonującego
- w ramach partycji trzymamy rekordy uporządkowane według klucza sortującego
- modelujemy pod konkretny odczyt, np. 'zamówienia użytkownika po dacie'

To NIE jest prawdziwa baza Cassandra/ScyllaDB.
To tylko bardzo krótka symulacja idei wide-column.
"""

# Tabela: orders_by_user
# partition key: user_id
# clustering key: order_date
orders_by_user = {
    101: [
        {"order_date": "2026-03-20", "order_id": 5001, "total": 120.50},
        {"order_date": "2026-03-22", "order_id": 5002, "total": 89.99},
        {"order_date": "2026-03-25", "order_id": 5003, "total": 210.00},
    ],
    102: [
        {"order_date": "2026-03-21", "order_id": 6001, "total": 45.00},
        {"order_date": "2026-03-24", "order_id": 6002, "total": 75.20},
    ],
}

# Odczyt wszystkich zamówień jednego użytkownika
user_id = 101
print(f"=== Zamówienia użytkownika {user_id} ===")
for order in orders_by_user.get(user_id, []):
    print(order)

# Dodanie nowego zamówienia
new_order = {"order_date": "2026-03-26", "order_id": 5004, "total": 59.90}
orders_by_user.setdefault(101, []).append(new_order)

# Sortowanie w obrębie partycji po dacie
orders_by_user[101].sort(key=lambda x: x["order_date"])

print(f"\n=== Po dodaniu nowego zamówienia dla użytkownika {user_id} ===")
for order in orders_by_user[101]:
    print(order)

# Przykład prostego zapytania: zamówienia od wskazanej daty
date_from = "2026-03-22"
filtered = [o for o in orders_by_user[101] if o["order_date"] >= date_from]

print(f"\n=== Zamówienia użytkownika {user_id} od {date_from} ===")
for order in filtered:
    print(order)

print("\nWniosek:")
print("W modelu wide-column dane układamy pod konkretny wzorzec odczytu, np. zamówienia użytkownika po dacie.")
