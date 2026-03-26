from dataclasses import dataclass, field
from typing import List, Dict, Any

@dataclass
class Address:
    city: str
    street: str
"""
class MyAddress:
    def __init__(self, city: str, street: str) -> None:
        self.city = city
        self.street = street'
"""

# CTRL + / - komentowanie - odkomentowanie bloku tekstu

opis = """
to jest definicja struktur danych
Python - dataclass
JSON - dict
"""
print(opis)

@dataclass
class User:
    user_id: int
    name: str
    email: str
    addresses: List[Dict[str, Any]] = field(default_factory=list)
    interests: List[str] = field(default_factory=list)
