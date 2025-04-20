import pandas as pd
import os
from typing import Optional

COLUMNS = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_timestamp",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore"
]

def detect_timestamp_unit(ts: int) -> int:
    """타임스탬프 유닛 자동 판별"""
    if ts > 1e15:
        return 1_000_000  # 마이크로초
    elif ts > 1e12:
        return 1_000      # 밀리초
    else:
        return 1          # 초

def convert_csv_timestamp(
    csv_path: str,
    output_path: Optional[str] = None,
    timestamp_col: int = 0,
    utc_to_kst: bool = False,
    overwrite: bool = True
):
    # CSV 읽기
    df = pd.read_csv(csv_path, header=None)

    # 의미 있는 컬럼명 지정
    if len(df.columns) < len(COLUMNS):
        raise ValueError("CSV 컬럼 수가 예상보다 적습니다.")
    
    df.columns = COLUMNS + [f'extra_{i}' for i in range(len(df.columns) - len(COLUMNS))]

    # 타임스탬프 처리
    sample_ts = df[COLUMNS[0]].iloc[0]
    divisor = detect_timestamp_unit(sample_ts)
    df['date'] = pd.to_datetime(df[COLUMNS[0]] // divisor, unit='s', utc=True)

    if utc_to_kst:
        df['date'] = df['date'].dt.tz_convert('Asia/Seoul')

    # 출력 경로
    if not output_path:
        base, ext = os.path.splitext(csv_path)
        output_path = f"{base}_converted{ext}"

    if not overwrite and os.path.exists(output_path):
        raise FileExistsError(f"파일이 이미 존재합니다: {output_path}")

    df.to_csv(output_path, index=False)
    print(f"✅ 변환 및 스키마 적용 완료: {output_path}")

# 🔽 사용 예시
if __name__ == '__main__':
    convert_csv_timestamp(
        csv_path='/Volumes/dev/ArbGuard_main/trade_simulate/1분가격/BTCUSDT-1m-2024-12.csv',
        utc_to_kst=False,
        overwrite=True
    )