import json
import pandas as pd


def main():
    # 1. Extract
    with open("events.json", "r", encoding="utf-8") as f:
        raw_events = json.load(f)

    # 2. Load do warstwy staging bez transformacji
    with open("staging_events.json", "w", encoding="utf-8") as f:
        json.dump(raw_events, f, indent=2, ensure_ascii=False)

    # 3. Transform po załadowaniu
    df = pd.DataFrame(raw_events)

    if "amount" not in df.columns:
        df["amount"] = 0
    df["amount"] = df["amount"].fillna(0)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["event_date"] = df["timestamp"].dt.date.astype(str)
    df["event_hour"] = df["timestamp"].dt.hour

    summary = df.groupby("user_id").agg(
        events_count=("event_id", "count"),
        purchases_count=("event_type", lambda x: int((x == "purchase").sum())),
        total_amount=("amount", "sum")
    ).reset_index()

    favorite_device = (
        df.groupby(["user_id", "device"])
          .size()
          .reset_index(name="count")
          .sort_values(["user_id", "count", "device"], ascending=[True, False, True])
          .drop_duplicates("user_id")[["user_id", "device"]]
          .rename(columns={"device": "favorite_device"})
    )

    result = summary.merge(favorite_device, on="user_id", how="left")

    # 4. Zapis wyniku docelowego
    result.to_json("user_activity_summary.json", orient="records", indent=2, force_ascii=False)

    print("Zapisano plik staging_events.json")
    print("Zapisano plik user_activity_summary.json")
    print(f"Liczba użytkowników w podsumowaniu: {len(result)}")


if __name__ == "__main__":
    main()
