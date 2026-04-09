import csv
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from pymongo import MongoClient, ReplaceOne
from pymongo.errors import BulkWriteError, DuplicateKeyError, PyMongoError


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "nosql_training")
BASE_DIR = Path(os.getenv("DATA_DIR", "."))


class PipelineError(Exception):
    pass


class NoSQLTrainingApp:
    def __init__(self, mongo_uri: str = MONGO_URI, db_name: str = MONGO_DB) -> None:
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]

    def setup_indexes(self) -> None:
        self.db["orders_etl"].create_index("order_id", unique=True)
        self.db["events_raw"].create_index("event_id", unique=True)
        self.db["user_activity_summary"].create_index("user_id", unique=True)

    def ping(self) -> None:
        self.client.admin.command("ping")

    def read_csv(self, path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            raise PipelineError(f"Brak pliku: {path}")
        with path.open("r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    def read_json(self, path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            raise PipelineError(f"Brak pliku: {path}")
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise PipelineError("Plik JSON musi zawierać listę obiektów.")
        return data

    def run_etl(self, data_dir: Path = BASE_DIR) -> None:
        customers = self.read_csv(data_dir / "customers.csv")
        orders = self.read_csv(data_dir / "orders.csv")
        items = self.read_csv(data_dir / "order_items.csv")

        customers_by_id: Dict[int, Dict[str, Any]] = {}
        for row in customers:
            if not row.get("customer_id"):
                raise PipelineError("W customers.csv wykryto pusty customer_id.")
            customer_id = int(row["customer_id"])
            customers_by_id[customer_id] = {
                "customer_id": customer_id,
                "name": row.get("name", "").strip(),
                "email": row.get("email", "").strip(),
                "city": row.get("city", "").strip(),
                "country": row.get("country", "").strip(),
            }

        items_by_order: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for row in items:
            if not row.get("order_id"):
                raise PipelineError("W order_items.csv wykryto pusty order_id.")

            order_id = int(row["order_id"])
            quantity = int(row.get("quantity", 0))
            unit_price = float(row.get("unit_price", 0))

            if quantity <= 0:
                raise PipelineError(f"Błędna quantity dla order_id={order_id}: {quantity}")
            if unit_price <= 0:
                raise PipelineError(f"Błędna unit_price dla order_id={order_id}: {unit_price}")

            item_doc = {
                "product": row.get("product", "").strip(),
                "category": row.get("category", "").strip(),
                "quantity": quantity,
                "unit_price": unit_price,
                "line_total": round(quantity * unit_price, 2),
            }
            items_by_order[order_id].append(item_doc)

        documents: List[Dict[str, Any]] = []
        for row in orders:
            if not row.get("order_id"):
                raise PipelineError("W orders.csv wykryto pusty order_id.")
            if not row.get("customer_id"):
                raise PipelineError("W orders.csv wykryto pusty customer_id.")

            order_id = int(row["order_id"])
            customer_id = int(row["customer_id"])

            if customer_id not in customers_by_id:
                raise PipelineError(
                    f"Brak klienta customer_id={customer_id} dla order_id={order_id}."
                )

            order_items = items_by_order.get(order_id, [])
            if not order_items:
                raise PipelineError(f"Brak pozycji dla order_id={order_id}.")

            doc = {
                "order_id": order_id,
                "order_date": row.get("order_date", "").strip(),
                "status": row.get("status", "").strip(),
                "customer": customers_by_id[customer_id],
                "items": order_items,
                "order_total": round(sum(i["line_total"] for i in order_items), 2),
                "loaded_at": datetime.utcnow(),
                "pipeline": "ETL",
            }
            documents.append(doc)

        collection = self.db["orders_etl"]
        collection.delete_many({})
        if documents:
            collection.insert_many(documents)

        print(f"ETL zakończony. Zapisano {len(documents)} dokumentów do kolekcji orders_etl.")

    def run_elt(self, data_dir: Path = BASE_DIR) -> None:
        events = self.read_json(data_dir / "events.json")

        raw_collection = self.db["events_raw"]
        summary_collection = self.db["user_activity_summary"]

        raw_collection.delete_many({})
        summary_collection.delete_many({})

        raw_docs: List[Dict[str, Any]] = []
        for event in events:
            if "event_id" not in event or "user_id" not in event or "event_type" not in event:
                raise PipelineError(f"Niekompletny event: {event}")
            raw_doc = dict(event)
            raw_doc["loaded_at"] = datetime.utcnow()
            raw_doc["pipeline"] = "ELT_STAGE"
            raw_docs.append(raw_doc)

        if raw_docs:
            raw_collection.insert_many(raw_docs)

        cursor = raw_collection.find({}, {"_id": 0, "loaded_at": 0, "pipeline": 0})
        grouped: Dict[int, Dict[str, Any]] = {}

        for event in cursor:
            user_id = int(event["user_id"])
            event_type = str(event.get("event_type", "")).strip()
            amount = float(event.get("amount", 0) or 0)
            device = str(event.get("device", "unknown")).strip() or "unknown"

            if user_id not in grouped:
                grouped[user_id] = {
                    "user_id": user_id,
                    "events_count": 0,
                    "purchases_count": 0,
                    "total_amount": 0.0,
                    "device_counter": Counter(),
                }

            grouped[user_id]["events_count"] += 1
            grouped[user_id]["device_counter"][device] += 1

            if event_type == "purchase":
                grouped[user_id]["purchases_count"] += 1
                grouped[user_id]["total_amount"] += amount

        summary_docs: List[Dict[str, Any]] = []
        for user_id, data in grouped.items():
            favorite_device = data["device_counter"].most_common(1)[0][0]
            summary_docs.append(
                {
                    "user_id": user_id,
                    "events_count": data["events_count"],
                    "purchases_count": data["purchases_count"],
                    "total_amount": round(data["total_amount"], 2),
                    "favorite_device": favorite_device,
                    "transformed_at": datetime.utcnow(),
                    "pipeline": "ELT_TARGET",
                }
            )

        if summary_docs:
            summary_collection.insert_many(summary_docs)

        print(f"ELT zakończony. Załadowano {len(raw_docs)} surowych zdarzeń do events_raw.")
        print(f"Utworzono {len(summary_docs)} rekordów podsumowania w user_activity_summary.")

    def show_results(self) -> None:
        print("\n=== orders_etl ===")
        for doc in self.db["orders_etl"].find({}, {"_id": 0}).limit(5):
            print(json.dumps(doc, ensure_ascii=False, indent=2, default=str))

        print("\n=== user_activity_summary ===")
        for doc in self.db["user_activity_summary"].find({}, {"_id": 0}).sort("user_id", 1):
            print(json.dumps(doc, ensure_ascii=False, indent=2, default=str))


SAMPLE_CUSTOMERS = """customer_id,name,email,city,country
1,Jan Kowalski,jan@example.com,Lublin,Poland
2,Anna Nowak,anna@example.com,Warsaw,Poland
3,Tom Smith,tom@example.com,Krakow,Poland
"""

SAMPLE_ORDERS = """order_id,customer_id,order_date,status
1001,1,2026-03-01,completed
1002,2,2026-03-02,pending
1003,1,2026-03-05,completed
"""

SAMPLE_ORDER_ITEMS = """order_id,product,category,quantity,unit_price
1001,Laptop,Electronics,1,4200
1001,Mouse,Electronics,2,80
1002,Chair,Furniture,1,600
1003,Monitor,Electronics,2,1200
"""

SAMPLE_EVENTS = [
    {"event_id": 1, "user_id": 101, "event_type": "page_view", "timestamp": "2026-03-10T10:15:00", "device": "mobile"},
    {"event_id": 2, "user_id": 101, "event_type": "add_to_cart", "timestamp": "2026-03-10T10:17:00", "device": "mobile"},
    {"event_id": 3, "user_id": 102, "event_type": "page_view", "timestamp": "2026-03-10T10:18:00", "device": "desktop"},
    {"event_id": 4, "user_id": 101, "event_type": "purchase", "timestamp": "2026-03-10T10:20:00", "device": "mobile", "amount": 249.99},
    {"event_id": 5, "user_id": 103, "event_type": "error", "timestamp": "2026-03-10T10:25:00", "device": "tablet"}
]


def write_sample_files(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "customers.csv").write_text(SAMPLE_CUSTOMERS, encoding="utf-8")
    (data_dir / "orders.csv").write_text(SAMPLE_ORDERS, encoding="utf-8")
    (data_dir / "order_items.csv").write_text(SAMPLE_ORDER_ITEMS, encoding="utf-8")
    (data_dir / "events.json").write_text(json.dumps(SAMPLE_EVENTS, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Zapisano pliki przykładowe do katalogu: {data_dir}")


def print_usage() -> None:
    print(
        "Użycie:\n"
        "  python app_mongo_etl_elt.py sample-data   # zapisuje przykładowe pliki wejściowe\n"
        "  python app_mongo_etl_elt.py etl           # uruchamia pipeline ETL\n"
        "  python app_mongo_etl_elt.py elt           # uruchamia pipeline ELT\n"
        "  python app_mongo_etl_elt.py all           # ETL + ELT + podgląd wyników\n"
    )


def main() -> int:
    if len(sys.argv) < 2:
        print_usage()
        return 1

    command = sys.argv[1].strip().lower()

    if command == "sample-data":
        write_sample_files(BASE_DIR)
        return 0

    app = NoSQLTrainingApp()

    try:
        app.ping()
        app.setup_indexes()

        if command == "etl":
            app.run_etl(BASE_DIR)
        elif command == "elt":
            app.run_elt(BASE_DIR)
        elif command == "all":
            app.run_etl(BASE_DIR)
            app.run_elt(BASE_DIR)
            app.show_results()
        else:
            print_usage()
            return 1

        return 0
    except (PipelineError, PyMongoError, OSError, ValueError) as exc:
        print(f"Błąd: {exc}")
        return 2
    finally:
        try:
            app.client.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
