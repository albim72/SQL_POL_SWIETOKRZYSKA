from __future__ import annotations

from typing import List, Optional

import pandas as pd
from sqlalchemy import ForeignKey, create_engine, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship


DB_URL = "sqlite:///firma_orm.db"


class Base(DeclarativeBase):
    pass


class Pracownik(Base):
    __tablename__ = "pracownicy"

    id: Mapped[int] = mapped_column(primary_key=True)
    imie: Mapped[str]
    dzial: Mapped[str]
    pensja: Mapped[float]

    projekty: Mapped[List[Projekt]] = relationship(back_populates="kierownik")

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "imie": self.imie,
            "dzial": self.dzial,
            "pensja": self.pensja,
        }


class Projekt(Base):
    __tablename__ = "projekty"

    projekt_id: Mapped[int] = mapped_column(primary_key=True)
    nazwa_projektu: Mapped[str]
    budzet: Mapped[float]
    kierownik_id: Mapped[Optional[int]] = mapped_column(ForeignKey("pracownicy.id"), nullable=True)

    kierownik: Mapped[Optional[Pracownik]] = relationship(back_populates="projekty")

    def as_dict(self) -> dict:
        return {
            "projekt_id": self.projekt_id,
            "nazwa_projektu": self.nazwa_projektu,
            "budzet": self.budzet,
            "kierownik_id": self.kierownik_id,
        }


def seed_data(session: Session) -> None:
    pracownicy = [
        Pracownik(id=1, imie="Anna", dzial="AI", pensja=12000),
        Pracownik(id=2, imie="Piotr", dzial="Data", pensja=9800),
        Pracownik(id=3, imie="Kasia", dzial="AI", pensja=13500),
        Pracownik(id=4, imie="Marek", dzial="DevOps", pensja=11000),
    ]

    projekty = [
        Projekt(projekt_id=101, nazwa_projektu="Agent AI", kierownik_id=1, budzet=50000),
        Projekt(projekt_id=102, nazwa_projektu="Migracja NoSQL", kierownik_id=2, budzet=32000),
        Projekt(projekt_id=103, nazwa_projektu="Predykcja sprzedaży", kierownik_id=3, budzet=76000),
    ]

    session.add_all(pracownicy + projekty)
    session.commit()


def main() -> None:
    engine = create_engine(DB_URL, echo=False)

    # Tworzenie tabel na podstawie klas ORM
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        seed_data(session)

        # 1. SELECT bez surowego SQL: pobranie wszystkich pracowników działu AI
        pracownicy_ai = session.scalars(
            select(Pracownik)
            .where(Pracownik.dzial == "AI")
            .order_by(Pracownik.pensja.desc())
        ).all()

        # 2. UPDATE bez SQL stringów: modyfikujemy obiekty
        for pracownik in pracownicy_ai:
            pracownik.pensja += 1500
        session.commit()

        # 3. INSERT bez SQL stringów
        nowy_pracownik = Pracownik(id=5, imie="Ewa", dzial="Data", pensja=10500)
        session.add(nowy_pracownik)
        session.commit()

        # 4. DELETE bez SQL stringów
        tani_projekt = session.scalar(
            select(Projekt).where(Projekt.budzet < 40000)
        )
        if tani_projekt is not None:
            session.delete(tani_projekt)
            session.commit()

        # 5. JOIN przez ORM
        join_rows = session.execute(
            select(
                Pracownik.imie,
                Pracownik.dzial,
                Pracownik.pensja,
                Projekt.nazwa_projektu,
                Projekt.budzet,
            ).outerjoin(Projekt, Pracownik.id == Projekt.kierownik_id)
             .order_by(Pracownik.pensja.desc())
        ).all()

        # 6. Agregacja przez SQLAlchemy expression API, nadal bez raw SQL
        agregacja_rows = session.execute(
            select(
                Pracownik.dzial,
                func.count(Pracownik.id).label("liczba_pracownikow"),
                func.avg(Pracownik.pensja).label("srednia_pensja"),
                func.max(Pracownik.pensja).label("max_pensja"),
            )
            .group_by(Pracownik.dzial)
            .order_by(func.avg(Pracownik.pensja).desc())
        ).all()

        # 7. Konwersja wyników ORM do pandas
        wszyscy_pracownicy = session.scalars(select(Pracownik).order_by(Pracownik.id)).all()
        pracownicy_df = pd.DataFrame.from_records([p.as_dict() for p in wszyscy_pracownicy])
        join_df = pd.DataFrame.from_records(join_rows, columns=["imie", "dzial", "pensja", "nazwa_projektu", "budzet"])
        agregacja_df = pd.DataFrame.from_records(
            agregacja_rows,
            columns=["dzial", "liczba_pracownikow", "srednia_pensja", "max_pensja"],
        )

        print("\n=== Pracownicy po zmianach ===")
        print(pracownicy_df)

        print("\n=== JOIN pracownicy + projekty ===")
        print(join_df)

        print("\n=== Agregacja po działach ===")
        print(agregacja_df)


if __name__ == "__main__":
    main()
