#!/usr/bin/env python3
"""simulate.py

* 변환기(convert_csv_timestamp)로 만든 `<이름>_converted.csv` 사용
* 지정 월(YYYY-MM) 전체 1 분 진입 → TP · LIQ · OPEN 판정
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
import numpy as np

# ───────────────── 전략 파라미터 ─────────────────
LEVERAGE = 20
ROI_NET  = 0.05
FEE_MAKER = 0.0002
INIT_M    = 2000
ADD_M     = 800
DROPS     = [0.01, 0.02, 0.03, 0.04]
MMR       = 0.005
# ────────────────────────────────────────────────

# ① 변환 CSV 로드 → time·price 두 열
def load_converted(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if 'date' not in df.columns or 'close' not in df.columns:
        raise ValueError("CSV 에 'date' 또는 'close' 컬럼이 없습니다.")
    df['time'] = pd.to_datetime(df['date'])  # UTC
    df['price'] = pd.to_numeric(df['close'], errors='coerce')
    return df[['time', 'price']].dropna().reset_index(drop=True)

# ② 물타기 계획
def make_plan(entry: float):
    r_gross = ROI_NET
    t = r_gross / LEVERAGE
    m = INIT_M
    n = INIT_M * LEVERAGE
    q = n / entry
    avg = entry
    plan = []
    plan.append(dict(step=0, trigger=entry, avg=avg, tp=avg * (1 + t), margin=m, qty=q))
    for d in DROPS:
        trigger_price = avg * (1 - d)
        m += ADD_M
        n += ADD_M * LEVERAGE
        q += (ADD_M * LEVERAGE) / trigger_price
        avg = n / q
        plan.append(dict(step=len(plan), trigger=trigger_price, avg=avg, tp=avg * (1 + t), margin=m, qty=q))
    return plan  # step: 0~4

# ③ 한 포지션 시뮬레이션
def simulate(df: pd.DataFrame, start: int) -> dict:
    entry = df.at[start, 'price']
    plan = make_plan(entry)
    pos = 0
    tp_price = plan[0]['tp']
    waters = []
    for i in range(start + 1, len(df)):
        price = df.at[i, 'price']
        timestamp = df.at[i, 'time']
        # TP 달성
        if price >= tp_price:
            return dict(res='TP', hold=i - start, exit=timestamp, waters=waters)
        # 물타기 단계 진입
        while pos < len(plan) - 1 and price <= plan[pos + 1]['trigger']:
            pos += 1
            waters.append(timestamp)
            tp_price = plan[pos]['tp']
        # LIQ 달성 조건 계산
        avg = plan[pos]['avg']
        qty = plan[pos]['qty']
        margin = plan[pos]['margin']
        liq_price = (avg * qty - margin) / (qty * (1 - MMR))
        if price <= liq_price:
            return dict(res='LIQ', hold=i - start, exit=timestamp, waters=waters)
    # 월말까지 OPEN
    return dict(res='OPEN', hold=len(df) - start - 1, exit=None, waters=waters)

# ④ 월간 백테스트
def backtest(df: pd.DataFrame, month: str, out_base: str):
    mdf = df[df['time'].dt.strftime('%Y-%m') == month].reset_index(drop=True)
    records = []
    for idx in range(len(mdf)):
        sim = simulate(mdf, idx)
        entry_price = mdf.at[idx, 'price']
        water_cnt = len(sim['waters'])
        plan = make_plan(entry_price)
        if sim['res'] == 'TP':
            profit = plan[water_cnt]['qty'] * (plan[water_cnt]['tp'] - plan[water_cnt]['avg'])
        else:
            profit = 0
        record = dict(
            Entry_Time=mdf.at[idx, 'time'],
            Entry_Price=entry_price,
            Result=sim['res'],
            Hold_Min=sim['hold'],
            Exit_Time=sim['exit'],
            WaterCnt=water_cnt,
            Profit=round(profit, 2)
        )
        # 물타기별 타임스탬프
        for j in range(len(DROPS)):
            record[f'Water{j+1}'] = sim['waters'][j] if j < len(sim['waters']) else None
        records.append(record)
        if idx % 3000 == 0:
            print(f'... {idx}/{len(mdf)} done')

    df_res = pd.DataFrame(records)

    # 파일 저장: TP+LIQ, LIQ, TP, OPEN
    df_res[df_res['Result'] != 'OPEN']           .to_csv(f"{out_base}.csv",    index=False)
    df_res[df_res['Result'] == 'LIQ']            .to_csv(f"{out_base}_liq.csv", index=False)
    df_res[df_res['Result'] == 'TP']             .to_csv(f"{out_base}_tp.csv",  index=False)
    df_res[df_res['Result'] == 'OPEN']           .to_csv(f"{out_base}_open.csv",index=False)

    # 통계 출력
    total = len(df_res)
    tp_rate   = df_res['Result'].eq('TP').mean()  * 100
    liq_rate  = df_res['Result'].eq('LIQ').mean() * 100
    open_rate = 100 - tp_rate - liq_rate

    # ➀ 물타기 단계별 평균 보유 시간
    hold_stats = (
        df_res.groupby('WaterCnt')['Hold_Min']
              .mean()
              .reset_index(name='AvgHoldMin')
    )
    print("\n— 물타기 단계별 평균 보유 시간 —")
    print(hold_stats.to_string(index=False))

    # ➁ 물타기 단계별 평균 수익 (USD)
    profit_stats = (
        df_res.groupby('WaterCnt')['Profit']
              .mean()
              .reset_index(name='AvgProfitUSD')
    )
    print("\n— 물타기 단계별 평균 수익 (USD) —")
    print(profit_stats.to_string(index=False))

    print('\n—— 승률 통계 ——')
    print(f"TP   : {tp_rate:5.2f}%  ({df_res['Result'].eq('TP').sum()} / {total})")
    print(f"LIQ  : {liq_rate:5.2f}%  ({df_res['Result'].eq('LIQ').sum()} / {total})")
    print(f"OPEN : {open_rate:5.2f}%  ({df_res['Result'].eq('OPEN').sum()} / {total})")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('converted_csv')
    parser.add_argument('--month', default='2025-03')
    parser.add_argument('--out',   default='march_sim_report')
    args = parser.parse_args()

    price_df = load_converted(args.converted_csv)
    backtest(price_df, args.month, os.path.splitext(args.out)[0])
