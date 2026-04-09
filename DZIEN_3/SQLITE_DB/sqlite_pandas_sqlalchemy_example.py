from __future__ import annotations

from sqlalchemy import create_engine, text
import pandas as pd


DB_URL = "sqlite:///firma_demo.db"


def main() -> None:
    # 1. Tworzymy engine SQLAlchemy dla SQLite
    engine = create_engine(DB_URL, echo=False)

    # 2. Dane wejściowe w pandas DataFrame
    pracownicy_df = pd.DataFrame(
        [
            {"id": 1, "imie": "Anna", "dzial": "AI", "pensja": 12000},
            {"id": 2, "imie": "Piotr", "dzial": "Data", "pensja": 9800},
            {"id": 3, "imie": "Kasia", "dzial": "AI", "pensja": 13500},
            {"id": 4, "imie": "Marek", "dzial": "DevOps", "pensja": 11000},
        ]
    )

    projekty_df = pd.DataFrame(
        [
            {"projekt_id": 101, "nazwa_projektu": "Agent AI", "kierownik_id": 1, "budzet": 50000},
            {"projekt_id": 102, "nazwa_projektu": "Migracja NoSQL", "kierownik_id": 2, "budzet": 32000},
            {"projekt_id": 103, "nazwa_projektu": "Predykcja sprzedaży", "kierownik_id": 3, "budzet": 76000},
        ]
    )

    # 3. Zapisujemy DataFrame do SQLite
    # if_exists='replace' nadpisuje tabelę przy każdym uruchomieniu
    pracownicy_df.to_sql("pracownicy", con=engine, if_exists="replace", index=False)
    projekty_df.to_sql("projekty", con=engine, if_exists="replace", index=False)

    # 4. Operacje SQL przez SQLAlchemy
    with engine.begin() as conn:
        # Podwyżka dla działu AI
        conn.execute(
            text(
                """
                UPDATE pracownicy
                SET pensja = pensja + :podwyzka
                WHERE dzial = :dzial
                """
            ),
            {"podwyzka": 1500, "dzial": "AI"},
        )

        # Dodanie nowego pracownika
        conn.execute(
            text(
                """
                INSERT INTO pracownicy (id, imie, dzial, pensja)
                VALUES (:id, :imie, :dzial, :pensja)
                """
            ),
            {"id": 5, "imie": "Ewa", "dzial": "Data", "pensja": 10500},
        )

        # Usunięcie projektu o zbyt małym budżecie
        conn.execute(
            text(
                """
                DELETE FROM projekty
                WHERE budzet < :prog
                """
            ),
            {"prog": 40000},
        )

    # 5. Odczyt całej tabeli do pandas
    pracownicy_po_zmianach = pd.read_sql("SELECT * FROM pracownicy", con=engine)

    # 6. Bardziej sensowne zapytanie: JOIN + agregacja
    raport_df = pd.read_sql(
        text(
            """
            SELECT
                p.id,
                p.imie,
                p.dzial,
                p.pensja,
                pr.nazwa_projektu,
                pr.budzet
            FROM pracownicy p
            LEFT JOIN projekty pr
                ON p.id = pr.kierownik_id
            ORDER BY p.pensja DESC
            """
        ),
        con=engine,
    )

    # 7. Agregacja po działach
    agregacja_df = pd.read_sql(
        text(
            """
            SELECT
                dzial,
                COUNT(*) AS liczba_pracownikow,
                AVG(pensja) AS srednia_pensja,
                MAX(pensja) AS max_pensja
            FROM pracownicy
            GROUP BY dzial
            ORDER BY srednia_pensja DESC
            """
        ),
        con=engine,
    )

    print("\n=== Tabela pracownicy po zmianach ===")
    print(pracownicy_po_zmianach)

    print("\n=== Raport JOIN: pracownicy + projekty ===")
    print(raport_df)

    print("\n=== Agregacja po działach ===")
    print(agregacja_df)


if __name__ == "__main__":
    main()
