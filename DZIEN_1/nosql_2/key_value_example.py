"""Szybki przykład bazy key-value w Pythonie.

Model:
- klucz -> wartość
- bardzo szybki dostęp po kluczu
- dobre do sesji, cache, tokenów, konfiguracji
"""

# Symulacja prostego magazynu key-value
session_store = {}

# CREATE / PUT
session_store["sess_001"] = {"user_id": 101, "role": "admin", "active": True}
session_store["sess_002"] = {"user_id": 102, "role": "user", "active": True}

print("=== Po dodaniu danych ===")
print(session_store)

# READ
print("\n=== Odczyt jednej sesji po kluczu ===")
print(session_store.get("sess_001"))

# UPDATE
session_store["sess_002"]["active"] = False

print("\n=== Po aktualizacji sesji sess_002 ===")
print(session_store["sess_002"])

# DELETE
del session_store["sess_001"]

print("\n=== Po usunięciu sess_001 ===")
print(session_store)

print("\nWniosek:")
print("W bazie key-value najważniejszy jest szybki dostęp do wartości po znanym kluczu.")
