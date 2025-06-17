#!/usr/bin/env python3
# shipment_tracker_plain.py

import json
import csv
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dateutil import parser
import pytz
import statistics


DATA_FILE = Path("Swift Assignment 4 - Dataset.json")
FLAT_CSV  = Path("shipments_flat.csv")
SUM_CSV   = Path("summary_stats.csv")


UTC = pytz.utc
IST = pytz.timezone("Asia/Kolkata")



def extractor(path):
    """Load JSON file and return Python object."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def parse_iso_to_ist(dt_str):
    """
    Parse an ISO8601 string to a timezone-aware datetime in IST.
    """
    dt_utc = parser.isoparse(dt_str)
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    dt_ist = dt_utc.astimezone(IST)
    return dt_ist

def millis_to_ist(ms_str):
    """
    Convert a stringified milliseconds-since-epoch to IST datetime.
    """
    ms = int(ms_str)
    dt_utc = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt_utc.astimezone(IST)

def format_dt(dt):
    """Format datetime as YYYY-MM-DD HH:MM:SS."""
    return dt.strftime("%Y-%m-%d %H:%M:%S")




def process_shipments(raw):
    """
    Process raw JSON list into flat records and collect summary values.
    Returns:
      flat_rows: list of dicts for each shipment
      days_list: list of ints (days taken)
      attempts_list: list of ints (delivery attempts)
    """
    flat_rows = []
    days_list = []
    attempts_list = []

    for entry in raw:
        details_list = entry.get("trackDetails") or []
        if not details_list:
            continue
        det = details_list[0]

        tn = det.get("trackingNumber", "")
   
        pay_types = [h for h in det.get("specialHandlings", []) if h.get("type") == "COD"]
        payment = "COD" if pay_types else "Prepaid"
        weight = det.get("shipmentWeight", {}).get("value", "")

        
        ship = det.get("shipperAddress", {})
        dest = det.get("destinationAddress", {})

        dt_map = {}
        for d in det.get("datesOrTimes", []):
            t = d.get("type")
            val = d.get("dateOrTimestamp")
            if t and val:
                dt_map[t] = val

        pickup_raw = dt_map.get("ACTUAL_PICKUP")
        delivery_raw = dt_map.get("ACTUAL_DELIVERY")
        if not pickup_raw or not delivery_raw:
            continue

        p_dt = parse_iso_to_ist(pickup_raw)
        d_dt = parse_iso_to_ist(delivery_raw)

       
        days = (d_dt.date() - p_dt.date()).days

      
        od_dates = set()
        dl_date = None

        for ev in det.get("events", []):
            et = ev.get("eventType")
            ts_obj = ev.get("timestamp", {})
            ms = ts_obj.get("$numberLong")
            if not ms:
                continue
            dt_ist = millis_to_ist(ms)
            if et == "OD":
                od_dates.add(dt_ist.date())
            elif et == "DL":
                dl_date = dt_ist.date()

      
        attempts = len(od_dates)  
        if dl_date and dl_date not in od_dates:
            attempts += 1

        
        pu_pin = None
        for ev in det.get("events", []):
            if ev.get("eventType") == "PU":
                addr = ev.get("address", {})
                pu_pin = addr.get("postalCode")
                break

        row = {
            "tracking_number": tn,
            "payment_type": payment,
            "pickup_datetime_ist": format_dt(p_dt),
            "delivery_datetime_ist": format_dt(d_dt),
            "days_taken": days,
            "shipment_weight": weight,
            "pickup_pincode": pu_pin or ship.get("postalCode", ""),
            "pickup_city": ship.get("city", ""),
            "pickup_state": ship.get("stateOrProvinceCode", ""),
            "drop_pincode": dest.get("postalCode", ""),
            "drop_city": dest.get("city", ""),
            "drop_state": dest.get("stateOrProvinceCode", ""),
            "delivery_attempts": attempts
        }

        flat_rows.append(row)
        days_list.append(days)
        attempts_list.append(attempts)

    return flat_rows, days_list, attempts_list




def write_flat_csv(rows, path):
    """Write the detailed shipments CSV."""
    headers = [
        "tracking_number",
        "payment_type",
        "pickup_datetime_ist",
        "delivery_datetime_ist",
        "days_taken",
        "shipment_weight",
        "pickup_pincode",
        "pickup_city",
        "pickup_state",
        "drop_pincode",
        "drop_city",
        "drop_state",
        "delivery_attempts"
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

def write_summary_csv(days, attempts, path):
    """Write the summary statistics CSV."""
  
    stats = [
        {
            "metric": "days_taken",
            "mean": statistics.mean(days),
            "median": statistics.median(days),
            "mode": statistics.mode(days)
        },
        {
            "metric": "delivery_attempts",
            "mean": statistics.mean(attempts),
            "median": statistics.median(attempts),
            "mode": statistics.mode(attempts)
        }
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["metric","mean","median","mode"])
        writer.writeheader()
        for s in stats:
            writer.writerow(s)




def main():
    print("Loading JSON…")
    raw_data = extractor(DATA_FILE)

    print("Processing shipments…")
    flat_rows, days_list, attempts_list = process_shipments(raw_data)

    print(f"Writing flat CSV to {FLAT_CSV}…")
    write_flat_csv(flat_rows, FLAT_CSV)

    print(f"Writing summary CSV to {SUM_CSV}…")
    write_summary_csv(days_list, attempts_list, SUM_CSV)

    print("Done.")

if __name__ == "__main__":
    main()
