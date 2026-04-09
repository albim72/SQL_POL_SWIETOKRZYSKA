# Przykłady ETL i ELT w Pythonie

Zestaw zawiera dwa osobne przykłady:

- `etl_example.py` - transformacja danych przed zapisem do wyniku JSON
- `elt_example.py` - najpierw zapis do warstwy staging, potem transformacja i agregacja

## Pliki wejściowe

### Dla ETL
- `customers.csv`
- `orders.csv`
- `order_items.csv`

### Dla ELT
- `events.json`

## Instalacja

```bash
python -m pip install -r requirements.txt
```

## Uruchomienie ETL

```bash
python etl_example.py
```

Wynik:
- `orders_nosql.json`

## Uruchomienie ELT

```bash
python elt_example.py
```

Wyniki:
- `staging_events.json`
- `user_activity_summary.json`

## Sens dydaktyczny

### ETL
- Extract: odczyt CSV
- Transform: czyszczenie, łączenie i przebudowa modelu
- Load: zapis do JSON

### ELT
- Extract: odczyt surowych logów
- Load: zapis do staging bez zmian
- Transform: agregacja i analiza po załadowaniu
