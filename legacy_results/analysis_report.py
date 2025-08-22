#!/usr/bin/env python3
"""
analysis_report.py

월단위 트레이딩 리포트(CSV)를 불러와 자동으로 통계를 내고 결과를 CSV로 저장합니다.

주요 기능:
 1. 날짜별 청산 확률 계산 및 CSV 저장
 2. 시간대별 청산 확률 계산(내림차순) 및 CSV 저장
 3. 여러 룩백(5m,10m,30m,1h,6h) 가격 변동 기준 TP/LIQ 승률 계산 및 CSV 저장
 4. 모든 필터(날짜, 시간대, 룩백) 적용 시 최대 TP 승률 계산 및 CSV 저장

Usage:
  python analysis_report.py \
    --liq <liq_csv> \
    --tp <tp_csv> \
    --price <price_csv> \
    --month YYYY-MM
"""
import argparse
import pandas as pd

# ── 유틸 함수 ─────────────────────────────────────────────

def loadReports(liqPath: str, tpPath: str) -> pd.DataFrame:
    """LIQ/TP 리포트 CSV를 로드하고 통합, KST 컬럼 추가"""
    liq = pd.read_csv(liqPath, parse_dates=['Entry_Time'])
    tp  = pd.read_csv(tpPath, parse_dates=['Entry_Time'])
    liq['Result'] = 'LIQ'
    tp['Result']  = 'TP'
    df = pd.concat([liq, tp], ignore_index=True)
    df['Entry_Time'] = pd.to_datetime(df['Entry_Time']).dt.tz_convert('Asia/Seoul')
    df['Date'] = df['Entry_Time'].dt.date
    df['Hour'] = df['Entry_Time'].dt.hour
    return df


def computeDateStats(df: pd.DataFrame, month: str) -> pd.DataFrame:
    mask = df['Entry_Time'].dt.strftime('%Y-%m') == month
    sub = df[mask]
    dateCounts = sub.groupby('Date').size().rename('Total')
    liqCounts  = sub[sub['Result']=='LIQ'].groupby('Date').size().rename('LIQ')
    stats = pd.concat([dateCounts, liqCounts], axis=1).fillna(0)
    stats['LIQ_Prob(%)'] = (stats['LIQ'] / stats['Total'] * 100).round(2)
    stats = stats.sort_index().reset_index()
    return stats[['Date','Total','LIQ','LIQ_Prob(%)']]


def computeHourStats(df: pd.DataFrame, month: str) -> pd.DataFrame:
    mask = df['Entry_Time'].dt.strftime('%Y-%m') == month
    sub = df[mask]
    hourCounts = sub.groupby('Hour').size().rename('Total')
    liqCounts  = sub[sub['Result']=='LIQ'].groupby('Hour').size().rename('LIQ')
    stats = pd.concat([hourCounts, liqCounts], axis=1).fillna(0)
    stats['LIQ_Prob(%)'] = (stats['LIQ'] / stats['Total'] * 100).round(2)
    stats = stats.sort_values('LIQ_Prob(%)', ascending=False).reset_index()
    return stats[['Hour','Total','LIQ','LIQ_Prob(%)']]


def computeLookbackStats(df: pd.DataFrame, pricePath: str, month: str) -> pd.DataFrame:
    priceDf = pd.read_csv(pricePath, parse_dates=['date'])
    priceDf.rename(columns={'date':'time','close':'price'}, inplace=True)
    priceDf.set_index('time', inplace=True)
    mask = df['Entry_Time'].dt.strftime('%Y-%m') == month
    sub = df[mask].copy()
    lookbacks = [5,10,30,60,360]
    records = []
    for lb in lookbacks:
        col = f'Ret_{lb}m'
        sub[col] = sub.apply(
            lambda r: ((r['Entry_Price'] - priceDf['price'].asof(r['Entry_Time'] - pd.Timedelta(minutes=lb)))
                       / priceDf['price'].asof(r['Entry_Time'] - pd.Timedelta(minutes=lb))) * 100,
            axis=1
        )
        bins = [-float('inf'), -1, -0.5, 0, 0.5, float('inf')]
        labels = ['<-1%','-1~-0.5','-0.5~0','0~0.5','>0.5%']
        bin_col = f'Bin_{lb}m'
        sub[bin_col] = pd.cut(sub[col], bins=bins, labels=labels)
        grp = sub.groupby(bin_col)['Result'].agg(
            Count='size',
            **{'TP_Rate(%)': lambda x: (x=='TP').sum()/len(x)*100}
        ).reset_index()
        grp['Lookback(min)'] = lb
        grp.rename(columns={bin_col:'ReturnBin'}, inplace=True)
        records.append(grp[['Lookback(min)','ReturnBin','Count','TP_Rate(%)']])
    return pd.concat(records, ignore_index=True)


def computeOptimizedTP(df: pd.DataFrame, pricePath: str, month: str) -> pd.DataFrame:
    mask = df['Entry_Time'].dt.strftime('%Y-%m') == month
    sub = df[mask].copy()
    topDates = sub[sub['Result']=='LIQ']['Date'].value_counts().head(3).index
    sub = sub[~sub['Date'].isin(topDates)]
    topHours = sub[sub['Result']=='LIQ']['Hour'].value_counts().head(2).index
    sub = sub[~sub['Hour'].isin(topHours)]
    priceDf = pd.read_csv(pricePath, parse_dates=['date'])
    priceDf.rename(columns={'date':'time','close':'price'}, inplace=True)
    priceDf.set_index('time', inplace=True)
    sub['Ret_30m'] = sub.apply(
        lambda r: ((r['Entry_Price'] - priceDf['price'].asof(r['Entry_Time'] - pd.Timedelta(minutes=30)))
                   / priceDf['price'].asof(r['Entry_Time'] - pd.Timedelta(minutes=30))) * 100,
        axis=1
    )
    sub = sub[sub['Ret_30m'] < 0]
    tpCount = (sub['Result']=='TP').sum()
    totalCount = len(sub)
    return pd.DataFrame([{'Filtered_TP_Rate(%)': round(tpCount/totalCount*100,2), 'Total_Entries': totalCount}])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--liq',   required=True)
    parser.add_argument('--tp',    required=True)
    parser.add_argument('--price', required=True)
    parser.add_argument('--month', required=True)
    args = parser.parse_args()

    df = loadReports(args.liq, args.tp)
    dateStats = computeDateStats(df, args.month)
    hourStats = computeHourStats(df, args.month)
    lookStats = computeLookbackStats(df, args.price, args.month)
    optStats  = computeOptimizedTP(df, args.price, args.month)

    dateStats.to_csv(f'date_stats_{args.month}.csv', index=False)
    hourStats.to_csv(f'hour_stats_{args.month}.csv', index=False)
    lookStats.to_csv(f'lookback_stats_{args.month}.csv', index=False)
    optStats.to_csv(f'optimized_tp_{args.month}.csv', index=False)

if __name__ == '__main__':
    main()
