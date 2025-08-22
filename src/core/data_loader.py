#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data loading utilities for trading simulation
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Optional


def detect_epoch_seconds(series: pd.Series) -> pd.Series:
    """Detect timestamp format and convert to seconds."""
    ot = series.astype("int64")
    v = int(ot.iloc[0])
    if v >= 10**18:      # nanoseconds
        return ot // 1_000_000_000
    elif v >= 10**15:    # microseconds
        return ot // 1_000_000
    elif v >= 10**12:    # milliseconds
        return ot // 1_000
    else:                # seconds
        return ot


def load_klines(csv_path: str) -> pd.DataFrame:
    """Load Binance kline CSV data and create time columns."""
    try:
        head = pd.read_csv(csv_path, nrows=3)
        if head.columns[0].lower() in ("open_time", "opentime"):
            df = pd.read_csv(csv_path)
            df.columns = [c.lower() for c in df.columns]
        else:
            raise ValueError("assume no header")
    except Exception:
        cols = ["open_time","open","high","low","close","volume",
                "close_time","quote_asset_volume","number_of_trades",
                "taker_buy_base_volume","taker_buy_quote_volume","ignore"]
        df = pd.read_csv(csv_path, header=None, names=cols)

    for c in ["open","high","low","close","volume"]:
        df[c] = df[c].astype(float)

    open_seconds = detect_epoch_seconds(df["open_time"])
    df["dt_utc"] = pd.to_datetime(open_seconds, unit="s", utc=True)
    df["dt_ny"]  = df["dt_utc"].dt.tz_convert("America/New_York")
    return df


def build_time_masks(df: pd.DataFrame) -> Dict[str, pd.Series]:
    """Build time-based masks for different trading scenarios."""
    ny = df["dt_ny"]
    wk = ny.dt.weekday  # Mon=0 ... Sun=6
    in_regular = ((wk <= 4) &
                  (ny.dt.time >= pd.Timestamp("09:30").time()) &
                  (ny.dt.time <  pd.Timestamp("16:00").time()))
    
    return {
        "weekend": (wk >= 5),
        "us_closed": ~in_regular,
        "all": pd.Series(True, index=df.index)
    }


def calculate_atr(df: pd.DataFrame, window: int = 60) -> pd.Series:
    """Calculate ATR percentage based on 1-hour window."""
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)
    pc = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    atr = tr.rolling(window, min_periods=window).mean()
    return (atr / c).bfill()
