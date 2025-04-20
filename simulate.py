
#!/usr/bin/env python3
"""simulate.py

* 변환기(convert_csv_timestamp)로 만든 `<이름>_converted.csv` 사용
* 지정 월(YYYY-MM) 전체 1 분 진입 → TP · LIQ · OPEN 판정
* 결과 CSV 3종 자동 저장
  1) <out>.csv            : TP + LIQ (OPEN 제외)
  2) <out>_liq.csv        : LIQ 전용
  3) <out>_open_raw.csv   : 월말까지 OPEN인 포지션을
                            원본(시각·가격) 형식으로 잘라낸 CSV
                            → 다음 달 CSV에 concat 해서 carry‑over 가능
* 콘솔에 TP / LIQ / OPEN 비율 통계 출력
"""

import pandas as pd, argparse, os, numpy as np

# ───────────────── 전략 파라미터 ─────────────────
LEVERAGE   = 20
ROI_NET    = 0.05
FEE_MAKER  = 0.0002
INIT_M     = 2000
ADD_M      = 800
DROPS      = [0.01,0.02,0.03,0.04]
MMR        = 0.005
# ────────────────────────────────────────────────

# ① 변환 CSV 로드 → time·price 두 열
def load_converted(path:str)->pd.DataFrame:
    df = pd.read_csv(path)
    if 'date' not in df.columns or 'close' not in df.columns:
        raise ValueError("CSV 에 'date' 또는 'close' 컬럼이 없습니다.")
    df['time']  = pd.to_datetime(df['date'])    # UTC
    df['price'] = pd.to_numeric(df['close'], errors='coerce')
    return df[['time','price']].dropna().reset_index(drop=True)

# ② 물타기 계획
def make_plan(entry:float):
    r_gross = ROI_NET + 2*FEE_MAKER*LEVERAGE
    t = r_gross / LEVERAGE
    rows=[]; m,n = INIT_M, INIT_M*LEVERAGE
    q = n/entry; avg=entry
    rows.append(dict(step=0,trigger=entry,avg=avg,tp=avg*(1+t),margin=m,qty=q))
    for d in DROPS:
        trg=avg*(1-d)
        m += ADD_M; n += ADD_M*LEVERAGE
        q += ADD_M*LEVERAGE / trg
        avg = n/q
        rows.append(dict(step=len(rows),trigger=trg,avg=avg,tp=avg*(1+t),margin=m,qty=q))
    return rows           # 0~4

# ③ 한 포지션 시뮬
def simulate(df,start):
    entry=df.at[start,'price']
    plan = make_plan(entry)
    pos=0; tp=plan[pos]['tp']; waters=[]
    for i in range(start+1,len(df)):
        p,ts = df.at[i,'price'], df.at[i,'time']
        if p>=tp:
            return dict(res='TP',hold=i-start,exit=ts,waters=waters)
        while pos<4 and p<=plan[pos+1]['trigger']:
            pos+=1; waters.append(ts); tp=plan[pos]['tp']
        avg,qty,margin=plan[pos]['avg'],plan[pos]['qty'],plan[pos]['margin']
        liq=(avg*qty - margin)/(qty*(1-MMR))
        if p<=liq:
            return dict(res='LIQ',hold=i-start,exit=ts,waters=waters)
    return dict(res='OPEN',hold=len(df)-start-1,exit=None,waters=waters)

# ④ 월간 백테스트
def backtest(df, month, out_base):
    mdf = df[df['time'].dt.strftime('%Y-%m')==month].reset_index(drop=True)
    recs=[]; open_idx=[]
    for idx in range(len(mdf)):
        sim=simulate(mdf,idx)
        rec=dict(Entry_Time=mdf.at[idx,'time'],
                 Entry_Price=mdf.at[idx,'price'],
                 Result=sim['res'],Hold_Min=sim['hold'],Exit_Time=sim['exit'])
        for j in range(4):
            rec[f'Water{j+1}']=sim['waters'][j] if j<len(sim['waters']) else None
        recs.append(rec)
        if sim['res']=='OPEN':
            open_idx.append(idx)
        if idx%3000==0: print(f'...{idx}/{len(mdf)} done')

    df_res=pd.DataFrame(recs)

    # 1) TP+LIQ
    df_res[df_res['Result']!='OPEN'].to_csv(out_base+'.csv',index=False)
    # 2) LIQ only
    df_res[df_res['Result']=='LIQ' ].to_csv(out_base+'_liq.csv',index=False)
    # 3) OPEN raw (원본 행 유지)
    mdf.loc[open_idx].to_csv(out_base+'_open_raw.csv',index=False)

    # 통계
    total=len(df_res)
    tp=(df_res['Result']=='TP').mean()*100
    liq=(df_res['Result']=='LIQ').mean()*100
    df_res['WaterCnt'] = df_res[['Water1','Water2','Water3','Water4']].notna().sum(axis=1)

    # ❷ 물타기 단계별 평균 보유 시간
    hold_stats = (
        df_res.groupby('WaterCnt')['Hold_Min']
              .mean()
              .rename('AvgHoldMin')
              .reset_index()
    )
    print("\n— 물타기 단계별 평균 보유 시간 —")
    print(hold_stats.to_string(index=False))
    print('\n——  승률 통계  ——')
    print(f'TP   : {tp:5.2f}%  ({(df_res["Result"]=="TP").sum()} / {total})')
    print(f'LIQ  : {liq:5.2f}%  ({(df_res["Result"]=="LIQ").sum()} / {total})')
    print(f'OPEN : {100-tp-liq:5.2f}%  ({(df_res["Result"]=="OPEN").sum()} / {total})')
    print(f'''✅ 저장:
  · {out_base}.csv
  · {out_base}_liq.csv
  · {out_base}_open_raw.csv''')

# ─── main ───
if __name__=='__main__':
    ap=argparse.ArgumentParser()
    ap.add_argument('converted_csv')
    ap.add_argument('--month',default='2025-03')
    ap.add_argument('--out',default='march_sim_report')
    args=ap.parse_args()

    price_df = load_converted(args.converted_csv)
    out_base = os.path.splitext(args.out)[0]
    
    backtest(price_df, args.month, out_base)

    