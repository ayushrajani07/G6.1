"""
update_master_overlays.py
-------------------------
Reads all raw CSV snapshots for the day and regenerates master overlay files.
No live API calls. Safe to run after market close.
"""

import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from config import load_storage_config

# Config
INDEX_SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]
DATE_FMT = "%Y-%m-%d"

def load_csv_for_index(storage_cfg, symbol, date_str):
    """
    Load all CSV rows for a given index and date.
    """
    csv_dir = Path(storage_cfg["csv_root"]) / symbol
    if not csv_dir.exists():
        return pd.DataFrame()

    # Match files for the given date
    files = list(csv_dir.glob(f"*{date_str}*.csv"))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            dfs.append(df)
        except Exception as e:
            print(f"[WARN] Could not read {f}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def generate_master_overlay(df):
    """
    Given a day's worth of raw snapshots for one index,
    compute the master overlay values.
    """
    if df.empty:
        return pd.DataFrame()

    # Example: take last snapshot of the day per (expiry, offset)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")
    overlay_df = df.groupby(["expiry", "offset"], as_index=False).last()

    return overlay_df

def save_master_overlay(storage_cfg, symbol, overlay_df, date_str):
    """
    Save the overlay DataFrame to the master directory.
    """
    master_dir = Path(storage_cfg["master_root"]) / symbol
    master_dir.mkdir(parents=True, exist_ok=True)
    out_file = master_dir / f"master_overlay_{date_str}.csv"
    overlay_df.to_csv(out_file, index=False)
    print(f"[OK] Saved master overlay for {symbol} → {out_file}")

def main():
    storage_cfg = load_storage_config()
    date_str = datetime.now().strftime(DATE_FMT)

    for symbol in INDEX_SYMBOLS:
        print(f"[INFO] Processing {symbol} for {date_str}...")
        df = load_csv_for_index(storage_cfg, symbol, date_str)
        overlay_df = generate_master_overlay(df)
        if overlay_df.empty:
            print(f"[SKIP] No data for {symbol} on {date_str}")
            continue
        save_master_overlay(storage_cfg, symbol, overlay_df, date_str)

if __name__ == "__main__":
    main()