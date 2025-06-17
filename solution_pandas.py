

import json
from pathlib import Path
from statistics import mean, median, mode

import pandas as pd
from dateutil import parser
import pytz



DATA_PATH = Path("Swift Assignment 4 - Dataset.json")
OUTPUT_DIR = Path("outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FLAT_CSV    = OUTPUT_DIR / "shipments_flat.csv"
SUMMARY_CSV = OUTPUT_DIR / "summary_stats.csv"

UTC = pytz.utc
IST = pytz.timezone("Asia/Kolkata")




def extractor(path: Path) -> list:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)




def flatten_shipments(raw_records: list) -> pd.DataFrame:
    rows = []
    for record in raw_records:
    
        details_list = record.get("trackDetails", [])
        if not details_list:
            continue
        details = details_list[0]

        tn      = details["trackingNumber"]
        weight  = details["shipmentWeight"]["value"]

       
        cod_flags = [h for h in details.get("specialHandlings", [])
                     if h.get("type") == "COD"]
        payment = "COD" if cod_flags else "Prepaid"

        
        dt_map = {
            entry["type"]: entry["dateOrTimestamp"]
            for entry in details.get("datesOrTimes", [])
        }
        pickup_ts   = dt_map.get("ACTUAL_PICKUP")
        delivery_ts = dt_map.get("ACTUAL_DELIVERY")


        events = details.get("events", [])
        od_timestamps = [
            int(e["timestamp"]["$numberLong"])
            for e in events
            if e.get("eventType") == "OD"
        ]
        dl_timestamp = int(next(
            e["timestamp"]["$numberLong"]
            for e in events
            if e.get("eventType") == "DL"
        ))

       
        ship = details["shipperAddress"]
        dest = details["destinationAddress"]

    
        pu_event = next((e for e in events if e.get("eventType") == "PU"), None)
        pickup_pincode = (pu_event["address"].get("postalCode")
                          if pu_event else None)

        rows.append({
            "tracking_number":   tn,
            "payment_type":      payment,
            "pickup_ts":         pickup_ts,
            "delivery_ts":       delivery_ts,
            "od_timestamps":     od_timestamps,
            "dl_timestamp":      dl_timestamp,
            "shipment_weight":   weight,
            "pickup_pincode":    pickup_pincode,
            "pickup_city":       ship.get("city"),
            "pickup_state":      ship.get("stateOrProvinceCode"),
            "drop_pincode":      dest.get("postalCode"),
            "drop_city":         dest.get("city"),
            "drop_state":        dest.get("stateOrProvinceCode"),
        })
    return pd.DataFrame(rows)



def parse_to_ist(ts: str) -> pd.Timestamp:
    """
    Parse an ISO8601 timestamp or numeric ms string,
    interpret as UTC, and convert to IST.
    """
 
    if ts.isdigit():
        dt_utc = pd.to_datetime(int(ts), unit="ms", utc=True)
    else:
        dt_utc = parser.isoparse(ts)
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=UTC)
        else:
            dt_utc = dt_utc.astimezone(UTC)

    return dt_utc.astimezone(IST)


def compute_days_and_attempts(df: pd.DataFrame) -> pd.DataFrame:
  
    df["pickup_dt_ist"]   = df["pickup_ts"].apply(parse_to_ist)
    df["delivery_dt_ist"] = df["delivery_ts"].apply(parse_to_ist)


    df["days_taken"] = (
        df["delivery_dt_ist"].dt.normalize()
      - df["pickup_dt_ist"].dt.normalize()
    ).dt.days

   
    def count_attempts(row):

        od_dates = {
            pd.to_datetime(ts, unit="ms", utc=True)
              .astimezone(IST)
              .date()
            for ts in row["od_timestamps"]
        }
        dl_date = (
            pd.to_datetime(row["dl_timestamp"], unit="ms", utc=True)
              .astimezone(IST)
              .date()
        )
        attempts = len(od_dates)
        
        if dl_date not in od_dates:
            attempts += 1
        return attempts

    df["delivery_attempts"] = df.apply(count_attempts, axis=1)
    return df



def write_outputs(df: pd.DataFrame):
    flat_cols = [
        "tracking_number", "payment_type",
        "pickup_dt_ist",   "delivery_dt_ist",
        "days_taken",      "shipment_weight",
        "pickup_pincode",  "pickup_city",    "pickup_state",
        "drop_pincode",    "drop_city",      "drop_state",
        "delivery_attempts"
    ]
    df.to_csv(FLAT_CSV, columns=flat_cols, index=False)

   
    stats = {
        "metric":  ["days_taken", "delivery_attempts"],
        "mean":    [mean(df["days_taken"]),        mean(df["delivery_attempts"])],
        "median":  [median(df["days_taken"]),      median(df["delivery_attempts"])],
        "mode":    [mode(df["days_taken"]),        mode(df["delivery_attempts"])]
    }
    pd.DataFrame(stats).to_csv(SUMMARY_CSV, index=False)



def main():
    print("Loading raw JSON…")
    raw = extractor(DATA_PATH)

    print(f"Flattening {len(raw)} shipments…")
    df = flatten_shipments(raw)

    print("Computing dates, days taken, and delivery attempts…")
    df = compute_days_and_attempts(df)

    print("Writing output CSVs…")
    write_outputs(df)

    print(f"Flat data saved to:    {FLAT_CSV}")
    print(f"Summary stats saved to: {SUMMARY_CSV}")


if __name__ == "__main__":
    main()
