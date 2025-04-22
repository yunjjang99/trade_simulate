#!/usr/bin/env python3
"""simulate_no_scale.py

* 지정 월(YYYY-MM) 전체 1분 진입 → TP · LIQ · OPEN 판정
* 물타기 없이 첫 진입만 사용
* 결과 CSV 4종 자동 저장
  1) <out>.csv      : TP + LIQ (OPEN 제외)
  2) <out>_liq.csv  : LIQ 전용
  3) <out>_tp.csv   : TP 전용
  4) <out>_open.csv : OPEN 전용
* 콘솔에 TP / LIQ / OPEN 비율 통계 출력
"""

import pandas as pd
import argparse
import os

# ───────────────── 전략 파라미터 ─────────────────
LEVERAGE = 20
ROI_NET  = 0.05    # 순수익률 목표 5%
MMR       = 0.005  # 유지증거금 비율
INIT_M    = 2000   # 초기 증거금
# ────────────────────────────────────────────────

def load_converted(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if 'date' not in df.columns or 'close' not in df.columns:
        raise ValueError("CSV에 'date' 또는 'close' 컬럼이 없습니다.")
    df['time'] = pd.to_datetime(df['date'])
    df['price'] = pd.to_numeric(df['close'], errors='coerce')
    return df[['time', 'price']].dropna().reset_index(drop=True)


def simulate_no_scale(df: pd.DataFrame, start: int) -> dict:
    entry = df.at[start, 'price']
    t = ROI_NET / LEVERAGE
    tp_price = entry * (1 + t)
    margin = INIT_M
    qty = (INIT_M * LEVERAGE) / entry
    avg = entry

    for i in range(start + 1, len(df)):
        price = df.at[i, 'price']
        timestamp = df.at[i, 'time']
        # TP 달성
        if price >= tp_price:
            return dict(res='TP', hold=i - start, exit=timestamp)
        # LIQ 달성 조건 계산
        liq_price = (avg * qty - margin) / (qty * (1 - MMR))
        if price <= liq_price:
            return dict(res='LIQ', hold=i - start, exit=timestamp)
    # 월말까지 OPEN
    return dict(res='OPEN', hold=len(df) - start - 1, exit=None)


def backtest_no_scale(df: pd.DataFrame, month: str, out_base: str):
    mdf = df[df['time'].dt.strftime('%Y-%m') == month].reset_index(drop=True)
    records = []

    for idx in range(len(mdf)):
        sim = simulate_no_scale(mdf, idx)
        record = dict(
            Entry_Time=mdf.at[idx, 'time'],
            Entry_Price=mdf.at[idx, 'price'],
            Result=sim['res'],
            Hold_Min=sim['hold'],
            Exit_Time=sim['exit']
        )
        records.append(record)
        if idx % 3000 == 0:
            print(f'... {idx}/{len(mdf)} done')

    df_res = pd.DataFrame(records)
    # 파일 저장
    df_res[df_res['Result'] != 'OPEN'].to_csv(f"{out_base}.csv",    index=False)
    df_res[df_res['Result'] == 'LIQ'].to_csv(f"{out_base}_liq.csv", index=False)
    df_res[df_res['Result'] == 'TP'].to_csv(f"{out_base}_tp.csv",  index=False)
    df_res[df_res['Result'] == 'OPEN'].to_csv(f"{out_base}_open.csv",index=False)

    total = len(df_res)
    tp_rate   = df_res['Result'].eq('TP').mean()  * 100
    liq_rate  = df_res['Result'].eq('LIQ').mean() * 100
    open_rate = 100 - tp_rate - liq_rate

    print(f"TP: {tp_rate:.2f}% | LIQ: {liq_rate:.2f}% | OPEN: {open_rate:.2f}%")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('converted_csv')
    parser.add_argument('--month', default='2025-03')
    parser.add_argument('--out',   default='march_no_scale_report')
    args = parser.parse_args()

    price_df = load_converted(args.converted_csv)
    backtest_no_scale(price_df, args.month, os.path.splitext(args.out)[0])
