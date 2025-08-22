# 마이그레이션 가이드

## 기존 파일에서 새로운 아키텍처로 전환

### 🔄 변경 사항

#### 1. 파일 구조 정리

- **기존**: 루트에 모든 `.py` 파일이 흩어져 있음
- **새로운**: `src/` 디렉토리 아래 체계적으로 구성

#### 2. 결과 파일 관리

- **기존**: 루트에 모든 결과 파일이 저장됨
- **새로운**: `results/` 디렉토리 아래 시나리오/월별로 체계적 저장

### 📁 기존 파일 정리

#### 삭제 가능한 파일들

```bash
# 기존 시뮬레이션 파일들 (기능이 main.py로 통합됨)
횡보장.py
simulate.py
연속매매.py
변동성계산.py
변환기.py
단순화.py
analysis_report.py

# 기존 결과 파일들 (새로운 구조로 재생성됨)
events_*.csv
equity_*.csv
equity_*.png
```

#### 보존할 파일들

```bash
# 원본 데이터
1분가격/

# 기존 분석 결과 (참고용)
2025_분석_결과_요약.md
분석/
더미/
```

### 🚀 새로운 아키텍처 사용법

#### 1. 단일 시뮬레이션

```bash
# 기존
python 횡보장.py --csv 1분가격/BTCUSDT-1m-2025-01.csv --scenario weekend

# 새로운
python main.py --mode single --csv 1분가격/BTCUSDT-1m-2025-01.csv --scenario weekend
```

#### 2. 배치 분석

```bash
# 기존: 수동으로 각 파일 실행
python 횡보장.py --csv 1분가격/BTCUSDT-1m-2025-01.csv --scenario weekend
python 횡보장.py --csv 1분가격/BTCUSDT-1m-2025-02.csv --scenario weekend
python 횡보장.py --csv 1분가격/BTCUSDT-1m-2025-03.csv --scenario weekend

# 새로운: 한 번에 모든 파일 처리
python main.py --mode batch --data-dir 1분가격 --scenarios weekend us_closed all
```

### 📊 결과 파일 비교

#### 기존 구조

```
trade_simulate/
├── events_weekend.csv
├── equity_weekend.csv
├── equity_weekend.png
├── events_us_closed.csv
├── equity_us_closed.csv
├── equity_us_closed.png
└── ...
```

#### 새로운 구조

```
trade_simulate/
├── results/
│   ├── weekend/
│   │   ├── 2025-01/
│   │   │   ├── events_weekend_2025-01.csv
│   │   │   ├── equity_weekend_2025-01.csv
│   │   │   ├── equity_weekend_2025-01.png
│   │   │   └── summary_weekend_2025-01.txt
│   │   ├── 2025-02/
│   │   └── 2025-03/
│   ├── us_closed/
│   └── all/
```

### 🔧 설정 관리

#### 기존: 명령행 파라미터

```bash
python 횡보장.py --leverage 12 --leg-margin 1000 --price-move-pct 0.0035
```

#### 새로운: 설정 파일 사용

```python
# configs/default_config.py
DEFAULT_CONFIG = StrategyConfig(
    leverage=12.0,
    leg_margin=1000.0,
    price_move_pct=0.0035,
    # ...
)
```

### 📈 성능 개선

1. **모듈화**: 코드 재사용성 향상
2. **설정 관리**: 파라미터 체계적 관리
3. **결과 정리**: 자동 디렉토리 생성 및 파일 정리
4. **배치 처리**: 대량 데이터 효율적 처리

### 🛠️ 마이그레이션 단계

1. **백업 생성**

   ```bash
   cp -r . ../trade_simulate_backup
   ```

2. **새로운 아키텍처 테스트**

   ```bash
   python main.py --mode single --csv 1분가격/BTCUSDT-1m-2025-01.csv --scenario weekend
   ```

3. **기존 파일 정리**

   ```bash
   # 기존 시뮬레이션 파일들 삭제
   rm 횡보장.py simulate.py 연속매매.py 변동성계산.py 변환기.py 단순화.py analysis_report.py

   # 기존 결과 파일들 삭제 (새로운 구조로 재생성)
   rm events_*.csv equity_*.csv equity_*.png
   ```

4. **새로운 구조로 재분석**
   ```bash
   python main.py --mode batch --data-dir 1분가격
   ```

### ✅ 검증 방법

1. **결과 비교**: 기존 결과와 새로운 결과가 동일한지 확인
2. **파일 구조**: `results/` 디렉토리 구조 확인
3. **기능 테스트**: 다양한 파라미터로 테스트 실행

### 🆘 문제 해결

#### Import 오류

```bash
# src 디렉토리가 Python path에 없는 경우
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```

#### 의존성 문제

```bash
# 가상환경 재설정
rm -rf venv
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 📝 주의사항

1. **기존 데이터 보존**: `1분가격/` 디렉토리는 반드시 보존
2. **점진적 전환**: 한 번에 모든 파일을 삭제하지 말고 단계적으로 진행
3. **백업 필수**: 마이그레이션 전 반드시 백업 생성
