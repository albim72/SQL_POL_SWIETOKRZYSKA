from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine

SQLITE_URL = "sqlite:///sprzedaz_demo.db"
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "migracja_demo"
MONGO_COLLECTION = "sprzedaz"


def przygotuj_przykladowe_dane_sqlite(sqlite_url: str) -> None:
    """Tworzy przykładową tabelę w SQLite z użyciem pandas + SQLAlchemy."""
    engine = create_engine(sqlite_url)

    df = pd.DataFrame(
        {
            "id": np.arange(1, 6, dtype=np.int64),
            "produkt": ["Laptop", "Monitor", "Klawiatura", "Mysz", "Stacja dokująca"],
            "cena": np.array([4200.0, 1200.0, 250.0, 140.0, 650.0], dtype=np.float64),
            "ilosc": np.array([3, 5, 12, 20, 4], dtype=np.int32),
            "region": ["PL-Wschód", "PL-Zachód", "PL-Wschód", "PL-Północ", "PL-Zachód"],
        }
    )

    # Zapisujemy surowe dane do SQLite
    df.to_sql("sprzedaz", con=engine, if_exists="replace", index=False)


def numpy_i_pandas_do_python(obj: Any) -> Any:
    """Zamienia typy NumPy/Pandas na typy natywne Pythona, przyjazne dla MongoDB/BSON."""
    if isinstance(obj, dict):
        return {k: numpy_i_pandas_do_python(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [numpy_i_pandas_do_python(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(numpy_i_pandas_do_python(v) for v in obj)

    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()

    if isinstance(obj, pd.Timestamp):
        return obj.to_pydatetime()
    if pd.isna(obj):
        return None

    return obj


def migracja_sqlite_do_mongo(
    sqlite_url: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_collection: str,
) -> None:
    # 1. Odczyt danych z SQLite do pandas DataFrame
    engine = create_engine(sqlite_url)
    df = pd.read_sql("sprzedaz", con=engine)

    # 2. Transformacje w pandas + NumPy
    df["wartosc"] = df["cena"] * df["ilosc"]
    df["cena_z_vat"] = np.round(df["cena"] * 1.23, 2)
    df["duze_zamowienie"] = np.where(df["ilosc"] >= 10, True, False)

    # Normalizacja wartości do przedziału 0..1
    min_w = df["wartosc"].min()
    max_w = df["wartosc"].max()
    if max_w != min_w:
        df["wartosc_norm"] = np.round((df["wartosc"] - min_w) / (max_w - min_w), 4)
    else:
        df["wartosc_norm"] = 1.0

    # Dodajemy pole z wektorem NumPy, które potem zapiszemy do Mongo jako listę
    df["cechy_numpy"] = [
        np.array([row.cena, row.ilosc, row.wartosc], dtype=np.float64)
        for row in df.itertuples(index=False)
    ]

    # Znacznik migracji
    df["migrowano_at"] = pd.Timestamp(datetime.utcnow())

    # 3. DataFrame -> lista dokumentów dla MongoDB
    rekordy = df.to_dict(orient="records")
    rekordy = [numpy_i_pandas_do_python(r) for r in rekordy]

    # 4. Zapis do MongoDB
    client = MongoClient(mongo_uri)
    collection = client[mongo_db][mongo_collection]

    collection.delete_many({})
    result = collection.insert_many(rekordy)

    print(f"Wstawiono {len(result.inserted_ids)} dokumentów do MongoDB.")

    # 5. Podgląd jednego dokumentu
    pierwszy = collection.find_one({}, {"_id": 0})
    print("\nPrzykładowy dokument w MongoDB:")
    print(json.dumps(pierwszy, ensure_ascii=False, indent=2, default=str))

    # 6. Prosty raport kontrolny
    print("\nRaport kontrolny:")
    print("Liczba rekordów w DataFrame:", len(df))
    print("Liczba dokumentów w MongoDB:", collection.count_documents({}))

    client.close()


if __name__ == "__main__":
    # Krok 1: tworzymy przykładową tabelę w SQLite
    przygotuj_przykladowe_dane_sqlite(SQLITE_URL)

    # Krok 2: migrujemy dane do MongoDB
    migracja_sqlite_do_mongo(
        sqlite_url=SQLITE_URL,
        mongo_uri=MONGO_URI,
        mongo_db=MONGO_DB,
        mongo_collection=MONGO_COLLECTION,
    )
