#!/usr/bin/env python3
"""
variability_signal_reversal_backtest.py

* 지정 월(YYYY-MM) 전체 1분봉 데이터에 기반하여 멀티타임프레임 하락율 진입 신호로 포지션 진입
* 한 포지션(no-scale)만 보유, TP/LIQ/OPEN 판정
* 롱 진입 후 종료 시점에 숏으로 반전, 숏 종료 시점에 롱으로 반전하여 반복
* 결과 CSV 저장 및 TP/LIQ/OPEN 건수, 수익 통계 출력
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
    return df[['time','price']].dropna().set_index('time')


def compute_signals(df: pd.DataFrame, thresholds: dict) -> pd.Series:
    signals = pd.Series(False, index=df.index)
    for tf, th in thresholds.items():
        past = df['price'].shift(tf)
        drop = (past - df['price']) / past
        signals |= (drop >= th)
    return signals


def run_reversal_backtest(df: pd.DataFrame, signals: pd.Series,
                          roi_net: float, leverage: int, init_m: float, mmr: float,
                          start_time: pd.Timestamp) -> pd.DataFrame:
    trades = []
    direction = 'long'
    idx = df.index.get_indexer([start_time], method='bfill')[0]
    total = len(df)
    while idx < total-1:
        entry_price = df['price'].iat[idx]
        # calculate TP and initial liq price
        t = roi_net / leverage
        tp_price = entry_price * (1 + t) if direction=='long' else entry_price * (1 - t)
        margin = init_m
        qty = margin * leverage / entry_price
        avg = entry_price
        res = 'OPEN'; hold=0; exit_time=None; exit_price=None
        # iterate until close
        for offset in range(idx+1, total):
            price = df['price'].iat[offset]
            ts = df.index[offset]
            hold = offset - idx
            # take profit
            if direction=='long' and price >= tp_price:
                res='TP'; exit_time=ts; exit_price=price; break
            if direction=='short' and price <= tp_price:
                res='TP'; exit_time=ts; exit_price=price; break
            # liquidation price
            if direction=='long':
                liq_price = (avg*qty - margin)/(qty*(1-mmr))
                if price <= liq_price:
                    res='LIQ'; exit_time=ts; exit_price=price; break
            else:
                # short liq: symmetric for simplicity
                liq_price = (margin + qty*avg)/(qty*(1+mmr))
                if price >= liq_price:
                    res='LIQ'; exit_time=ts; exit_price=price; break
        # record
        trades.append({
            'Entry_Time': df.index[idx],
            'Entry_Price': entry_price,
            'Direction': direction,
            'Result': res,
            'Hold_Min': hold,
            'Exit_Time': exit_time,
            'Exit_Price': exit_price
        })
        # prepare next
        direction = 'short' if direction=='long' else 'long'
        # move idx to next signal or just after exit
        if exit_time is None:
            break
        # find next valid signal at or after exit_time
        next_idx = signals.index.get_indexer([exit_time], method='bfill')[0]
        idx = next_idx
    return pd.DataFrame(trades)


def backtest(path: str, month: str, out_base: str, thresholds: dict,
             roi_net: float, leverage: int, init_m: float, mmr: float,
             start_time: str):
    df = load_converted(path)
    df_month = df[df.index.strftime('%Y-%m')==month]
    signals = compute_signals(df_month, thresholds)
    start_ts = pd.to_datetime(start_time)
    start_ts = start_ts.tz_localize('UTC')
    trades = run_reversal_backtest(df_month, signals, roi_net, leverage, init_m, mmr, start_ts)
    # save
    trades.to_csv(f"{out_base}_trades.csv", index=False)
    # stats
    total=len(trades)
    tp=(trades['Result']=='TP').mean()*100
    liq=(trades['Result']=='LIQ').mean()*100
    openp=(trades['Result']=='OPEN').mean()*100
    print(f"Trades: {total}, TP: {tp:.2f}%, LIQ: {liq:.2f}%, OPEN: {openp:.2f}%")


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('converted_csv')
    parser.add_argument('--month', default='2025-03')
    parser.add_argument('--out',   default='reversal_backtest')
    parser.add_argument('--start', default='2025-03-01 01:01')
    parser.add_argument('--th5',   type=float, default=0.01)
    parser.add_argument('--th10',  type=float, default=0.015)
    parser.add_argument('--th30',  type=float, default=0.02)
    parser.add_argument('--th60',  type=float, default=0.025)
    parser.add_argument('--th360', type=float, default=0.03)
    parser.add_argument('--leverage',type=int, default=20)
    parser.add_argument('--roi_net', type=float, default=0.05)
    parser.add_argument('--init_m',  type=float, default=2000)
    parser.add_argument('--mmr',     type=float, default=0.005)
    args=parser.parse_args()
    thr={5:args.th5,10:args.th10,30:args.th30,60:args.th60,360:args.th360}
    backtest(args.converted_csv,args.month,args.out,thr,args.roi_net,args.leverage,args.init_m,args.mmr,args.start)
