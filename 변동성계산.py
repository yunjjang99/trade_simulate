#!/usr/bin/env python3
"""variability_signal_backtest.py

* 지정 월(YYYY-MM) 전체 1분봉 데이터로 멀티타임프레임 하락율 기반 진입 후 TP/LIQ/OPEN 판정
* 물타기 없이 단일 진입(no-scale)
* 하락율 임계치(thresholds)는 5, 10, 30, 60, 360분 기준으로 설정
* 결과 CSV 4종 자동 저장:
  1) <out>.csv      : TP+LIQ (OPEN 제외)
  2) <out>_liq.csv  : LIQ 전용
  3) <out>_tp.csv   : TP 전용
  4) <out>_open.csv : OPEN 전용
* 콘솔에 TP/LIQ/OPEN 비율 출력
"""
import os
import argparse
import pandas as pd

# ─────────────────────────────────────────
def load_converted(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if 'date' not in df.columns or 'close' not in df.columns:
        raise ValueError("CSV에 'date' 또는 'close' 컬럼이 없습니다.")
    df['time'] = pd.to_datetime(df['date'])
    df['price'] = pd.to_numeric(df['close'], errors='coerce')
    df = df[['time', 'price']].dropna().set_index('time')
    return df


def compute_signals(df: pd.DataFrame, thresholds: dict) -> pd.Series:
    """
    각 타임프레임 하락율이 threshold 이상일 때 Long 진입 신호(True)
    thresholds 예시: {5:0.01, 10:0.015, 30:0.02, 60:0.025, 360:0.03}
    """
    signals = pd.Series(False, index=df.index)
    for tf, th in thresholds.items():
        past = df['price'].shift(tf)
        drop = (past - df['price']) / past
        signals |= (drop >= th)
    return signals


def simulate_no_scale(df: pd.DataFrame, signals: pd.Series, roi_net: float, leverage: int, init_m: float, mmr: float) -> pd.DataFrame:
    records = []
    df_list = df.reset_index()
    idxs = [i for i, sig in enumerate(signals) if sig]
    for start in idxs:
        entry_price = df_list.at[start, 'price']
        t = roi_net / leverage
        tp_price = entry_price * (1 + t)
        margin = init_m
        qty = (init_m * leverage) / entry_price
        avg = entry_price
        res, hold, exit_time = 'OPEN', None, None
        for i in range(start + 1, len(df_list)):
            price = df_list.at[i, 'price']
            timestamp = df_list.at[i, 'time']
            if price >= tp_price:
                res, hold, exit_time = 'TP', i - start, timestamp
                break
            liq_price = (avg * qty - margin) / (qty * (1 - mmr))
            if price <= liq_price:
                res, hold, exit_time = 'LIQ', i - start, timestamp
                break
        if res == 'OPEN':
            hold = len(df_list) - start - 1
        records.append({
            'Entry_Time': df_list.at[start, 'time'],
            'Entry_Price': entry_price,
            'Result': res,
            'Hold_Min': hold,
            'Exit_Time': exit_time
        })
    return pd.DataFrame(records)


def backtest(df_csv: str, month: str, out_base: str, thresholds: dict, roi_net: float, leverage: int, init_m: float, mmr: float):
    df = load_converted(df_csv)
    df_month = df[df.index.strftime('%Y-%m') == month]
    signals = compute_signals(df_month, thresholds)
    df_res = simulate_no_scale(df_month, signals, roi_net, leverage, init_m, mmr)
    # CSV 저장
    df_res[df_res['Result'] != 'OPEN'].to_csv(f"{out_base}.csv", index=False)
    df_res[df_res['Result'] == 'LIQ'].to_csv(f"{out_base}_liq.csv", index=False)
    df_res[df_res['Result'] == 'TP'].to_csv(f"{out_base}_tp.csv", index=False)
    df_res[df_res['Result'] == 'OPEN'].to_csv(f"{out_base}_open.csv", index=False)
    # 통계 출력
    total = len(df_res)
    tp_rate = df_res['Result'].eq('TP').mean() * 100
    liq_rate = df_res['Result'].eq('LIQ').mean() * 100
    open_rate = 100 - tp_rate - liq_rate
    print(f"Entries: {total}, TP: {tp_rate:.2f}%, LIQ: {liq_rate:.2f}%, OPEN: {open_rate:.2f}%")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Contrarian backtest no-scale')
    parser.add_argument('converted_csv', help='Converted CSV file path')
    parser.add_argument('--month',    default='2025-03', help='YYYY-MM 형식')
    parser.add_argument('--out',      default='signal_no_scale', help='출력 파일 기본명')
    parser.add_argument('--th5',   type=float, default=0.01,  help='5분 하락 임계')
    parser.add_argument('--th10',  type=float, default=0.015, help='10분 하락 임계')
    parser.add_argument('--th30',  type=float, default=0.02,  help='30분 하락 임계')
    parser.add_argument('--th60',  type=float, default=0.025, help='60분 하락 임계')
    parser.add_argument('--th360', type=float, default=0.03,  help='360분 하락 임계')
    parser.add_argument('--leverage', type=int,   default=20,   help='레버리지')
    parser.add_argument('--roi_net',  type=float, default=0.05, help='순수익 목표')
    parser.add_argument('--init_m',   type=float, default=2000, help='초기 증거금')
    parser.add_argument('--mmr',      type=float, default=0.005, help='유지증거금 비율')
    args = parser.parse_args()

    thr = {5: args.th5, 10: args.th10, 30: args.th30, 60: args.th60, 360: args.th360}
    backtest(
        args.converted_csv, args.month, os.path.splitext(args.out)[0],
        thr, args.roi_net, args.leverage, args.init_m, args.mmr
    )
