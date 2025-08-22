#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
simulate.py
-----------
1분봉 데이터로 '롱·숏 동시보유 → 수익난 쪽 익절(+반대 레그 부분축소) → 기준가 복귀 시 재오픈' 전략을 백테스트합니다.

예시 실행:
  python simula.py --csv BTCUSDT-1m-2025-03.csv --scenario weekend \
    --leverage 12 --leg-margin 1000 --price-move-pct 0.0035 \
    --partial-reduce 0.30 --atr-factor 0.4 --preclose-min 45

시나리오:
  weekend    : 뉴욕시간 토/일만
  us_closed  : 미국 주식 정규장(09:30~16:00) 외 시간
  all        : 시간 필터 없이 전체 (ATR 게이팅은 그대로 적용)

가정:
  - 수수료/펀딩 = 0 (요청 전제)
  - TP는 캔들 OHLC로 판정(틱 경로 미모델링)
  - 복귀 감지 시 즉시(같은 캔들) 재오픈 허용
출력:
  - events_<scenario>.csv : 이벤트 로그(각 이벤트 net_pnl_change 포함 → 승/패 계산 가능)
  - equity_<scenario>.csv : 시간별 누적 손익
  - equity_<scenario>.png : 에쿼티 곡선
  - 콘솔 요약(승률/손익)
"""

import argparse
from datetime import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ------------------------------- Utils -------------------------------- #

def _detect_epoch_seconds(series: pd.Series) -> pd.Series:
    """ns/us/ms/seconds 여부를 값의 크기로 판별해 초 단위로 변환."""
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
    """Binance kline CSV(헤더 유/무 모두) 로딩 후 시간열 생성."""
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

    open_seconds = _detect_epoch_seconds(df["open_time"])
    df["dt_utc"] = pd.to_datetime(open_seconds, unit="s", utc=True)
    df["dt_ny"]  = df["dt_utc"].dt.tz_convert("America/New_York")
    return df


def build_masks(df: pd.DataFrame):
    ny = df["dt_ny"]
    wk = ny.dt.weekday  # Mon=0 ... Sun=6
    in_regular = ((wk <= 4) &
                  (ny.dt.time >= time(9,30)) &
                  (ny.dt.time <  time(16,0)))
    return {
        "weekend": (wk >= 5),
        "us_closed": ~in_regular,
        "all": pd.Series(True, index=df.index)
    }


def atr_pct_1h(df: pd.DataFrame, window: int = 60) -> pd.Series:
    """고전적 TR 기반 60분 ATR%."""
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)
    pc = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    atr = tr.rolling(window, min_periods=window).mean()
    return (atr / c).bfill()


# ---------------------------- Strategy Core ---------------------------- #

def simulate(
    df: pd.DataFrame,
    base_mask: pd.Series,
    leverage: float = 12.0,
    leg_margin: float = 1000.0,
    price_move_pct: float = 0.0035,    # 0.35%
    partial_reduce: float = 0.30,      # 반대 레그 즉시 축소 비율
    atr_pct: pd.Series | None = None,
    atr_factor: float = 0.4,           # atr_factor * ATR% <= price_move_pct일 때만 가동
    preclose_minutes: int = 45,
    immediate_reopen: bool = True
):
    if atr_pct is None:
        atr_pct = atr_pct_1h(df)
    allowed = (base_mask) & ((atr_factor * atr_pct) <= price_move_pct)
    allowed = allowed.fillna(False)

    # 연속 True 구간(세그먼트) 추출
    segments = []
    idx = df.index
    i = 0
    n = len(idx)
    while i < n:
        if not allowed.iloc[i]:
            i += 1
            continue
        start = i
        while i < n and allowed.iloc[i]:
            i += 1
        end = i - 1
        segments.append((start, end))

    total_pnl = 0.0
    wins = 0
    losses = 0
    preclose_resets = 0
    forced_closes = 0

    equity_times = []
    equity_values = []
    events = []

    for (s, e) in segments:
        seg = df.iloc[s:e+1].copy()
        if seg.empty:
            continue

        seg_end = seg.iloc[-1]["dt_utc"]
        cutoff = seg_end - pd.Timedelta(minutes=preclose_minutes)

        # 초기 상태: 양 레그 동시 보유
        state = "both_open"
        base_price = float(seg.iloc[0]["open"])
        long_size = 1.0
        short_size = 1.0
        no_new_open = False

        for _, row in seg.iterrows():
            t = row["dt_utc"]
            o = float(row["open"]); h = float(row["high"]); l = float(row["low"]); c = float(row["close"])

            if t >= cutoff:
                no_new_open = True

            up = base_price * (1.0 + price_move_pct)
            dn = base_price * (1.0 - price_move_pct)

            if state == "both_open":
                hit_up = (h >= up)
                hit_dn = (l <= dn)
                if hit_up or hit_dn:
                    # 상/하단 동시 터치 시, 시가에 더 가까운 쪽 먼저 체결로 간주
                    if hit_up and hit_dn:
                        first_up = abs(up - o) <= abs(o - dn)
                    else:
                        first_up = hit_up and not hit_dn

                    if first_up:
                        # 롱 TP 이익 - 숏 부분 축소 손실
                        pnl_win  = leg_margin * leverage * price_move_pct * long_size
                        pnl_loss = leg_margin * leverage * price_move_pct * (partial_reduce * short_size)
                        net = pnl_win - pnl_loss
                        total_pnl += net
                        if net > 0:
                            wins += 1
                        else:
                            losses += 1
                        short_size *= (1.0 - partial_reduce)
                        events.append({
                            "time": t, "event": "TP_long", "entry": base_price, "tp_price": up,
                            "pnl_win": round(pnl_win, 6), "pnl_loss": round(-pnl_loss, 6),
                            "net_pnl_change": round(net, 6),
                            "long_size_after": long_size, "short_size_after": short_size,
                            "pnl_cum": round(total_pnl, 6)
                        })
                        state = "short_only" if short_size > 0 else ("flat" if no_new_open else "both_open")
                        if state == "both_open":
                            base_price = o
                            long_size = short_size = 1.0
                            events.append({"time": t, "event": "instant_reopen", "price": base_price, "pnl_cum": round(total_pnl, 6)})
                        equity_times.append(t); equity_values.append(total_pnl)

                    else:
                        # 숏 TP 이익 - 롱 부분 축소 손실
                        pnl_win  = leg_margin * leverage * price_move_pct * short_size
                        pnl_loss = leg_margin * leverage * price_move_pct * (partial_reduce * long_size)
                        net = pnl_win - pnl_loss
                        total_pnl += net
                        if net > 0:
                            wins += 1
                        else:
                            losses += 1
                        long_size *= (1.0 - partial_reduce)
                        events.append({
                            "time": t, "event": "TP_short", "entry": base_price, "tp_price": dn,
                            "pnl_win": round(pnl_win, 6), "pnl_loss": round(-pnl_loss, 6),
                            "net_pnl_change": round(net, 6),
                            "long_size_after": long_size, "short_size_after": short_size,
                            "pnl_cum": round(total_pnl, 6)
                        })
                        state = "long_only" if long_size > 0 else ("flat" if no_new_open else "both_open")
                        if state == "both_open":
                            base_price = o
                            long_size = short_size = 1.0
                            events.append({"time": t, "event": "instant_reopen", "price": base_price, "pnl_cum": round(total_pnl, 6)})
                        equity_times.append(t); equity_values.append(total_pnl)

            if state == "short_only":
                # 기준가 복귀(= l<=base_price) 시 0손익 청산, 즉시 재오픈 가능
                if l <= base_price:
                    events.append({"time": t, "event": "revert_close_short", "revert_price": base_price,
                                   "pnl_change": 0.0, "pnl_cum": round(total_pnl, 6)})
                    short_size = 0.0
                    if (not no_new_open) and immediate_reopen:
                        state = "both_open"
                        base_price = o
                        long_size = short_size = 1.0
                        events.append({"time": t, "event": "immediate_reopen", "price": base_price, "pnl_cum": round(total_pnl, 6)})
                    else:
                        state = "flat"

            elif state == "long_only":
                if h >= base_price:
                    events.append({"time": t, "event": "revert_close_long", "revert_price": base_price,
                                   "pnl_change": 0.0, "pnl_cum": round(total_pnl, 6)})
                    long_size = 0.0
                    if (not no_new_open) and immediate_reopen:
                        state = "both_open"
                        base_price = o
                        long_size = short_size = 1.0
                        events.append({"time": t, "event": "immediate_reopen", "price": base_price, "pnl_cum": round(total_pnl, 6)})
                    else:
                        state = "flat"

            # 프리클로즈 창에서 단독 레그 정리
            if no_new_open and state in ("short_only", "long_only"):
                if state == "short_only":
                    pnl_leg = leg_margin * leverage * ((base_price - c) / base_price) * short_size
                    total_pnl += pnl_leg
                    if pnl_leg > 0:
                        wins += 1
                    else:
                        losses += 1
                    events.append({"time": t, "event": "preclose_reset_short", "exit": c,
                                   "size": short_size, "net_pnl_change": round(pnl_leg, 6), "pnl_cum": round(total_pnl, 6)})
                    short_size = 0.0
                else:
                    pnl_leg = leg_margin * leverage * ((c - base_price) / base_price) * long_size
                    total_pnl += pnl_leg
                    if pnl_leg > 0:
                        wins += 1
                    else:
                        losses += 1
                    events.append({"time": t, "event": "preclose_reset_long", "exit": c,
                                   "size": long_size, "net_pnl_change": round(pnl_leg, 6), "pnl_cum": round(total_pnl, 6)})
                    long_size = 0.0
                preclose_resets += 1
                state = "flat"
                equity_times.append(t); equity_values.append(total_pnl)

        # 세그먼트 종료 시 남아있다면 강제정리(이 경우는 프리클로즈로 거의 제거됨)
        last = seg.iloc[-1]
        lc = float(last["close"]); lt = last["dt_utc"]
        if state == "short_only" and short_size > 0:
            pnl_leg = leg_margin * leverage * ((base_price - lc) / base_price) * short_size
            total_pnl += pnl_leg
            if pnl_leg > 0:
                wins += 1
            else:
                losses += 1
            forced_closes += 1
            events.append({"time": lt, "event": "force_close_short", "exit": lc,
                           "size": short_size, "net_pnl_change": round(pnl_leg, 6), "pnl_cum": round(total_pnl, 6)})
            equity_times.append(lt); equity_values.append(total_pnl)
        elif state == "long_only" and long_size > 0:
            pnl_leg = leg_margin * leverage * ((lc - base_price) / base_price) * long_size
            total_pnl += pnl_leg
            if pnl_leg > 0:
                wins += 1
            else:
                losses += 1
            forced_closes += 1
            events.append({"time": lt, "event": "force_close_long", "exit": lc,
                           "size": long_size, "net_pnl_change": round(pnl_leg, 6), "pnl_cum": round(total_pnl, 6)})
            equity_times.append(lt); equity_values.append(total_pnl)

    # 결과물 구성
    equity_df = pd.DataFrame({"time": equity_times, "cum_pnl": equity_values})
    events_df = pd.DataFrame(events)

    total_events = max(1, wins + losses)
    win_rate = wins / total_events

    summary = {
        "wins_positive_events": wins,
        "loss_negative_events": losses,
        "win_rate": round(win_rate, 4),
        "preclose_resets": preclose_resets,
        "forced_closes": forced_closes,
        "total_realized_pnl_usd": round(total_pnl, 2),
    }
    return summary, equity_df, events_df


# ------------------------------ Runner -------------------------------- #

def run(args):
    df = load_klines(args.csv)
    masks = build_masks(df)
    if args.scenario not in masks:
        raise ValueError(f"Unknown scenario: {args.scenario}")
    base_mask = masks[args.scenario]

    atrp = atr_pct_1h(df)
    summary, equity_df, events_df = simulate(
        df,
        base_mask=base_mask,
        leverage=args.leverage,
        leg_margin=args.leg_margin,
        price_move_pct=args.price_move_pct,
        partial_reduce=args.partial_reduce,
        atr_pct=atrp,
        atr_factor=args.atr_factor,
        preclose_minutes=args.preclose_min,
        immediate_reopen=True
    )

    tag = args.scenario
    events_path = f"events_{tag}.csv"
    equity_path = f"equity_{tag}.csv"
    plot_path = f"equity_{tag}.png"

    events_df.to_csv(events_path, index=False)
    equity_df.to_csv(equity_path, index=False)

    # Equity curve
    plt.figure()
    if not equity_df.empty:
        plt.plot(equity_df["time"], equity_df["cum_pnl"])
    plt.title(f"Equity Curve - {tag}")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Cumulative PnL (USD)")
    plt.tight_layout()
    plt.savefig(plot_path, dpi=130)

    # Console summary
    print("=== Strategy Summary ===")
    for k, v in summary.items():
        print(f"{k}: {v}")
    print(f"\nSaved: {events_path}, {equity_path}, {plot_path}")


def parse_args():
    p = argparse.ArgumentParser(description="Hedged mean-reversion backtest on 1m klines.")
    p.add_argument("--csv", required=True, help="1분봉 CSV 경로 (Binance kline 스키마 또는 유사).")
    p.add_argument("--scenario", choices=["weekend","us_closed","all"], default="weekend")
    p.add_argument("--leverage", type=float, default=12.0)
    p.add_argument("--leg-margin", dest="leg_margin", type=float, default=1000.0)
    p.add_argument("--price-move-pct", dest="price_move_pct", type=float, default=0.0035)
    p.add_argument("--partial-reduce", dest="partial_reduce", type=float, default=0.30)
    p.add_argument("--atr-factor", dest="atr_factor", type=float, default=0.4)
    p.add_argument("--preclose-min", dest="preclose_min", type=int, default=45)
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())