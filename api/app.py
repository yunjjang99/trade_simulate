#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI Backend for Trading Strategy Simulation
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.core.data_loader import load_klines, build_time_masks, calculate_atr
from src.strategies.mean_reversion import MeanReversionStrategy, StrategyConfig
from src.utils.output import ResultManager
import pandas as pd
import json

app = FastAPI(title="Trading Strategy API", version="1.0.0")

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # NextJS 개발 서버
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic 모델들
class StrategyParams(BaseModel):
    leverage: float = 12.0
    leg_margin: float = 1000.0
    price_move_pct: float = 0.0035
    partial_reduce: float = 0.30
    atr_factor: float = 0.4
    preclose_minutes: int = 45
    immediate_reopen: bool = True

class SimulationRequest(BaseModel):
    csv_file: str
    scenario: str
    params: StrategyParams

class SimulationResponse(BaseModel):
    summary: Dict[str, Any]
    equity_data: List[Dict[str, Any]]
    events_data: List[Dict[str, Any]]
    success: bool
    message: str

@app.get("/")
async def root():
    return {"message": "Trading Strategy API"}

@app.get("/available-data")
async def get_available_data():
    """사용 가능한 데이터 파일 목록을 반환"""
    data_dir = Path("../1분가격")
    if not data_dir.exists():
        return {"files": []}
    
    csv_files = list(data_dir.glob("*.csv"))
    files = []
    for file in csv_files:
        if "converted" not in file.name:  # converted 파일 제외
            month = file.stem.split('-')[-2] + '-' + file.stem.split('-')[-1]
            files.append({
                "filename": file.name,
                "month": month,
                "path": str(file)
            })
    
    return {"files": sorted(files, key=lambda x: x["month"])}

@app.get("/scenarios")
async def get_scenarios():
    """사용 가능한 시나리오 목록을 반환"""
    return {
        "scenarios": [
            {"id": "weekend", "name": "Weekend (토/일)", "description": "뉴욕시간 토요일, 일요일만 거래"},
            {"id": "us_closed", "name": "US Closed", "description": "미국 주식장 외 시간"},
            {"id": "all", "name": "All Time", "description": "전체 시간 (ATR 게이팅 적용)"}
        ]
    }

@app.post("/simulate", response_model=SimulationResponse)
async def run_simulation(request: SimulationRequest):
    """시뮬레이션 실행"""
    try:
        # CSV 파일 경로 확인
        csv_path = Path(f"../1분가격/{request.csv_file}")
        if not csv_path.exists():
            raise HTTPException(status_code=404, detail=f"CSV file not found: {request.csv_file}")
        
        # 데이터 로딩
        df = load_klines(str(csv_path))
        masks = build_time_masks(df)
        
        if request.scenario not in masks:
            raise HTTPException(status_code=400, detail=f"Invalid scenario: {request.scenario}")
        
        base_mask = masks[request.scenario]
        atr_pct = calculate_atr(df)
        
        # 전략 설정
        config = StrategyConfig(
            leverage=request.params.leverage,
            leg_margin=request.params.leg_margin,
            price_move_pct=request.params.price_move_pct,
            partial_reduce=request.params.partial_reduce,
            atr_factor=request.params.atr_factor,
            preclose_minutes=request.params.preclose_minutes,
            immediate_reopen=request.params.immediate_reopen
        )
        
        # 시뮬레이션 실행
        strategy = MeanReversionStrategy(config)
        result = strategy.simulate(df, base_mask, atr_pct)
        
        # 결과 데이터 변환
        equity_data = []
        if not result.equity_df.empty:
            for _, row in result.equity_df.iterrows():
                equity_data.append({
                    "time": row["time"].isoformat(),
                    "cum_pnl": float(row["cum_pnl"])
                })
        
        events_data = []
        if not result.events_df.empty:
            for _, row in result.events_df.iterrows():
                event_dict = {}
                for col in result.events_df.columns:
                    value = row[col]
                    if pd.isna(value):
                        event_dict[col] = None
                    elif isinstance(value, pd.Timestamp):
                        event_dict[col] = value.isoformat()
                    else:
                        event_dict[col] = value
                events_data.append(event_dict)
        
        return SimulationResponse(
            summary=result.summary,
            equity_data=equity_data,
            events_data=events_data,
            success=True,
            message="Simulation completed successfully"
        )
        
    except Exception as e:
        return SimulationResponse(
            summary={},
            equity_data=[],
            events_data=[],
            success=False,
            message=f"Simulation failed: {str(e)}"
        )

@app.get("/batch-simulate")
async def batch_simulate(
    scenario: str,
    leverage: float = 12.0,
    leg_margin: float = 1000.0,
    price_move_pct: float = 0.0035,
    partial_reduce: float = 0.30,
    atr_factor: float = 0.4,
    preclose_minutes: int = 45
):
    """배치 시뮬레이션 실행"""
    try:
        data_dir = Path("../1분가격")
        csv_files = list(data_dir.glob("BTCUSDT-1m-2025-*.csv"))
        
        if not csv_files:
            return {"success": False, "message": "No CSV files found"}
        
        results = {}
        
        for csv_file in sorted(csv_files):
            month = csv_file.stem.split('-')[-2] + '-' + csv_file.stem.split('-')[-1]
            
            try:
                # 데이터 로딩
                df = load_klines(str(csv_file))
                masks = build_time_masks(df)
                
                if scenario not in masks:
                    continue
                
                base_mask = masks[scenario]
                atr_pct = calculate_atr(df)
                
                # 전략 설정
                config = StrategyConfig(
                    leverage=leverage,
                    leg_margin=leg_margin,
                    price_move_pct=price_move_pct,
                    partial_reduce=partial_reduce,
                    atr_factor=atr_factor,
                    preclose_minutes=preclose_minutes,
                    immediate_reopen=True
                )
                
                # 시뮬레이션 실행
                strategy = MeanReversionStrategy(config)
                result = strategy.simulate(df, base_mask, atr_pct)
                
                results[month] = {
                    "summary": result.summary,
                    "success": True
                }
                
            except Exception as e:
                results[month] = {
                    "summary": {},
                    "success": False,
                    "error": str(e)
                }
        
        return {
            "success": True,
            "results": results,
            "message": "Batch simulation completed"
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"Batch simulation failed: {str(e)}"
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
