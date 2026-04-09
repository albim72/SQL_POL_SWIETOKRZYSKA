from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Iterable, Sequence

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.diagnostics import ProgressBar
from pymongo import MongoClient


DEFAULT_CSV = "orders_5m.csv"
DEFAULT_MONGO_URI = "mongodb://localhost:27017/"
DEFAULT_DB = "bigdata_demo"
DEFAULT_COLLECTION = "orders"

REGIONS = np.array(["North", "South", "East", "West", "Central"], dtype=object)
CATEGORIES = np.array(
    ["Laptop", "Monitor", "Phone", "Tablet", "Accessory", "Printer"],
    dtype=object,
)
CHANNELS = np.array(["online", "retail", "partner"], dtype=object)

CSV_DTYPES = {
    "order_id": "int64",
    "customer_id": "int64",
    "product_category": "object",
    "region": "object",
    "sales_channel": "object",
    "quantity": "int64",
    "unit_price": "float64",
    "discount": "float64",
    "revenue": "float64",
    "order_date": "object",
}

MONGO_COLUMNS = [
    "order_id",
    "customer_id",
    "product_category",
    "region",
    "sales_channel",
    "quantity",
    "unit_price",
    "discount",
    "revenue",
    "order_date",
]


def build_meta(columns: Sequence[str] | None = None) -> pd.DataFrame:
    columns = list(columns) if columns is not None else list(MONGO_COLUMNS)
    meta_map = {
        "order_id": pd.Series([], dtype="int64"),
        "customer_id": pd.Series([], dtype="int64"),
        "product_category": pd.Series([], dtype="object"),
        "region": pd.Series([], dtype="object"),
        "sales_channel": pd.Series([], dtype="object"),
        "quantity": pd.Series([], dtype="int64"),
        "unit_price": pd.Series([], dtype="float64"),
        "discount": pd.Series([], dtype="float64"),
        "revenue": pd.Series([], dtype="float64"),
        "order_date": pd.Series([], dtype="object"),
    }
    return pd.DataFrame({col: meta_map[col] for col in columns})


def generate_csv(
    csv_path: str,
    total_rows: int = 5_000_000,
    chunk_size: int = 250_000,
    seed: int = 42,
) -> None:
    """Generate a synthetic CSV file in chunks so memory use stays bounded."""
    path = Path(csv_path)
    if path.exists():
        path.unlink()

    rng = np.random.default_rng(seed)
    start_date = np.datetime64("2024-01-01")

    rows_written = 0
    header = True

    while rows_written < total_rows:
        current_chunk = min(chunk_size, total_rows - rows_written)

        order_id = np.arange(rows_written + 1, rows_written + current_chunk + 1, dtype=np.int64)
        customer_id = rng.integers(10_000, 2_000_000, size=current_chunk, dtype=np.int64)
        product_category = rng.choice(CATEGORIES, size=current_chunk)
        region = rng.choice(REGIONS, size=current_chunk)
        sales_channel = rng.choice(CHANNELS, size=current_chunk, p=[0.55, 0.30, 0.15])
        quantity = rng.integers(1, 8, size=current_chunk, dtype=np.int64)
        unit_price = rng.uniform(20.0, 5000.0, size=current_chunk).round(2)
        discount = rng.choice(np.array([0.0, 0.05, 0.10, 0.15, 0.20]), size=current_chunk)
        revenue = (quantity * unit_price * (1.0 - discount)).round(2)
        day_offsets = rng.integers(0, 731, size=current_chunk, dtype=np.int64)
        order_date = pd.to_datetime(start_date + day_offsets.astype("timedelta64[D]")).strftime("%Y-%m-%d")

        df = pd.DataFrame(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "product_category": product_category,
                "region": region,
                "sales_channel": sales_channel,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount": discount,
                "revenue": revenue,
                "order_date": order_date,
            }
        )

        mode = "w" if header else "a"
        df.to_csv(path, index=False, mode=mode, header=header)
        header = False
        rows_written += current_chunk
        print(f"[GEN] zapisano {rows_written:,}/{total_rows:,} rekordów do {csv_path}")



def load_csv_to_mongo(
    csv_path: str,
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    batch_size: int = 100_000,
) -> None:
    """Load CSV to MongoDB in chunks."""
    client = MongoClient(mongo_uri)
    collection = client[db_name][collection_name]

    collection.drop()

    total_inserted = 0
    for chunk in pd.read_csv(csv_path, chunksize=batch_size, dtype=CSV_DTYPES):
        records = chunk.to_dict(orient="records")
        if records:
            collection.insert_many(records, ordered=False)
            total_inserted += len(records)
            print(f"[MONGO] wstawiono {total_inserted:,} rekordów")

    collection.create_index([("order_id", 1)], unique=True)
    collection.create_index([("region", 1)])
    collection.create_index([("product_category", 1)])
    collection.create_index([("sales_channel", 1)])

    print(f"[MONGO] liczba dokumentów w kolekcji: {collection.count_documents({}):,}")
    client.close()



def read_mongo_partition(
    start_id: int,
    stop_id: int,
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Read one range partition from MongoDB into pandas."""
    use_columns = list(columns) if columns is not None else list(MONGO_COLUMNS)
    projection = {column: 1 for column in use_columns}
    projection["_id"] = 0

    client = MongoClient(mongo_uri)
    collection = client[db_name][collection_name]

    cursor = collection.find(
        {"order_id": {"$gte": int(start_id), "$lt": int(stop_id)}},
        projection=projection,
    ).sort("order_id", 1)

    rows = list(cursor)
    client.close()

    if not rows:
        return build_meta(use_columns)

    df = pd.DataFrame(rows)
    return df[use_columns]



def make_dask_dataframe_from_mongo(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    partition_size: int = 250_000,
) -> dd.DataFrame:
    client = MongoClient(mongo_uri)
    collection = client[db_name][collection_name]
    total_docs = collection.count_documents({})
    client.close()

    if total_docs == 0:
        raise ValueError("Kolekcja MongoDB jest pusta. Najpierw załaduj dane.")

    starts = list(range(1, total_docs + 1, partition_size))
    stops = [min(start + partition_size, total_docs + 1) for start in starts]

    meta = build_meta()

    ddf = dd.from_map(
        read_mongo_partition,
        starts,
        stops,
        args=(mongo_uri, db_name, collection_name),
        meta=meta,
        enforce_metadata=True,
    )
    return ddf



def aggregate_with_dask(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    partition_size: int = 250_000,
    output_dir: str = "outputs",
) -> None:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    ddf = make_dask_dataframe_from_mongo(
        mongo_uri=mongo_uri,
        db_name=db_name,
        collection_name=collection_name,
        partition_size=partition_size,
    )

    agg_region = ddf.groupby("region").agg(
        {
            "order_id": "count",
            "quantity": ["sum", "mean"],
            "revenue": ["sum", "mean", "max"],
        },
        split_out=4,
    )

    agg_category_channel = ddf.groupby(["product_category", "sales_channel"]).agg(
        {
            "order_id": "count",
            "quantity": "sum",
            "revenue": "sum",
        },
        split_out=4,
    )

    with ProgressBar():
        region_df = agg_region.compute().reset_index()
        category_channel_df = agg_category_channel.compute().reset_index()

    region_df.columns = [
        "region",
        "orders_count",
        "quantity_sum",
        "quantity_mean",
        "revenue_sum",
        "revenue_mean",
        "revenue_max",
    ]

    category_channel_df.columns = [
        "product_category",
        "sales_channel",
        "orders_count",
        "quantity_sum",
        "revenue_sum",
    ]

    region_path = output / "agg_by_region.csv"
    category_path = output / "agg_by_category_channel.csv"

    region_df.sort_values("revenue_sum", ascending=False).to_csv(region_path, index=False)
    category_channel_df.sort_values("revenue_sum", ascending=False).to_csv(category_path, index=False)

    print("\n[DASK] agregacja po regionie:")
    print(region_df.sort_values("revenue_sum", ascending=False).to_string(index=False))

    print("\n[DASK] agregacja po kategorii i kanale sprzedaży:")
    print(category_channel_df.sort_values("revenue_sum", ascending=False).to_string(index=False))

    print(f"\nZapisano raporty do: {region_path} oraz {category_path}")



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generator CSV 5 mln rekordów -> MongoDB -> agregacja Dask"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_gen = subparsers.add_parser("generate", help="Generuj plik CSV")
    p_gen.add_argument("--csv", default=DEFAULT_CSV)
    p_gen.add_argument("--rows", type=int, default=5_000_000)
    p_gen.add_argument("--chunk-size", type=int, default=250_000)
    p_gen.add_argument("--seed", type=int, default=42)

    p_load = subparsers.add_parser("load", help="Załaduj CSV do MongoDB")
    p_load.add_argument("--csv", default=DEFAULT_CSV)
    p_load.add_argument("--mongo-uri", default=DEFAULT_MONGO_URI)
    p_load.add_argument("--db", default=DEFAULT_DB)
    p_load.add_argument("--collection", default=DEFAULT_COLLECTION)
    p_load.add_argument("--batch-size", type=int, default=100_000)

    p_agg = subparsers.add_parser("aggregate", help="Agreguj dane z MongoDB przez Dask")
    p_agg.add_argument("--mongo-uri", default=DEFAULT_MONGO_URI)
    p_agg.add_argument("--db", default=DEFAULT_DB)
    p_agg.add_argument("--collection", default=DEFAULT_COLLECTION)
    p_agg.add_argument("--partition-size", type=int, default=250_000)
    p_agg.add_argument("--output-dir", default="outputs")

    p_all = subparsers.add_parser("all", help="Wykonaj cały pipeline")
    p_all.add_argument("--csv", default=DEFAULT_CSV)
    p_all.add_argument("--rows", type=int, default=5_000_000)
    p_all.add_argument("--chunk-size", type=int, default=250_000)
    p_all.add_argument("--seed", type=int, default=42)
    p_all.add_argument("--mongo-uri", default=DEFAULT_MONGO_URI)
    p_all.add_argument("--db", default=DEFAULT_DB)
    p_all.add_argument("--collection", default=DEFAULT_COLLECTION)
    p_all.add_argument("--batch-size", type=int, default=100_000)
    p_all.add_argument("--partition-size", type=int, default=250_000)
    p_all.add_argument("--output-dir", default="outputs")

    return parser.parse_args()



def main() -> None:
    args = parse_args()

    if args.command == "generate":
        generate_csv(
            csv_path=args.csv,
            total_rows=args.rows,
            chunk_size=args.chunk_size,
            seed=args.seed,
        )
    elif args.command == "load":
        load_csv_to_mongo(
            csv_path=args.csv,
            mongo_uri=args.mongo_uri,
            db_name=args.db,
            collection_name=args.collection,
            batch_size=args.batch_size,
        )
    elif args.command == "aggregate":
        aggregate_with_dask(
            mongo_uri=args.mongo_uri,
            db_name=args.db,
            collection_name=args.collection,
            partition_size=args.partition_size,
            output_dir=args.output_dir,
        )
    elif args.command == "all":
        generate_csv(
            csv_path=args.csv,
            total_rows=args.rows,
            chunk_size=args.chunk_size,
            seed=args.seed,
        )
        load_csv_to_mongo(
            csv_path=args.csv,
            mongo_uri=args.mongo_uri,
            db_name=args.db,
            collection_name=args.collection,
            batch_size=args.batch_size,
        )
        aggregate_with_dask(
            mongo_uri=args.mongo_uri,
            db_name=args.db,
            collection_name=args.collection,
            partition_size=args.partition_size,
            output_dir=args.output_dir,
        )
    else:
        raise ValueError(f"Nieznana komenda: {args.command}")


if __name__ == "__main__":
    main()
