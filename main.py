#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main trading simulation runner
"""

import argparse
from pathlib import Path
from src.core.data_loader import load_klines, build_time_masks, calculate_atr
from src.strategies.mean_reversion import MeanReversionStrategy, StrategyConfig
from src.utils.output import ResultManager, print_summary


def run_simulation(csv_path: str, scenario: str, config: StrategyConfig, 
                  output_dir: str = "results") -> dict:
    """Run a single simulation."""
    # Load data
    df = load_klines(csv_path)
    masks = build_time_masks(df)
    
    if scenario not in masks:
        raise ValueError(f"Unknown scenario: {scenario}")
    
    base_mask = masks[scenario]
    atr_pct = calculate_atr(df)
    
    # Run strategy
    strategy = MeanReversionStrategy(config)
    result = strategy.simulate(df, base_mask, atr_pct)
    
    # Extract month from filename
    month = Path(csv_path).stem.split('-')[-2] + '-' + Path(csv_path).stem.split('-')[-1]
    
    # Save results
    result_manager = ResultManager(output_dir)
    saved_paths = result_manager.save_results(result, scenario, month)
    
    # Print summary
    print_summary(result, scenario, month)
    
    return {
        "summary": result.summary,
        "paths": saved_paths
    }


def run_batch_analysis(data_dir: str, scenarios: list, config: StrategyConfig, 
                      output_dir: str = "results"):
    """Run batch analysis for multiple months and scenarios."""
    data_path = Path(data_dir)
    csv_files = list(data_path.glob("BTCUSDT-1m-2025-*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {data_dir}")
        return
    
    results = {}
    
    for csv_file in sorted(csv_files):
        month = csv_file.stem.split('-')[-2] + '-' + csv_file.stem.split('-')[-1]
        print(f"\n{'='*50}")
        print(f"Processing {month}")
        print(f"{'='*50}")
        
        results[month] = {}
        
        for scenario in scenarios:
            try:
                result = run_simulation(str(csv_file), scenario, config, output_dir)
                results[month][scenario] = result
            except Exception as e:
                print(f"Error processing {month} - {scenario}: {e}")
                results[month][scenario] = {"error": str(e)}
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Trading Strategy Simulation")
    parser.add_argument("--mode", choices=["single", "batch"], default="single",
                       help="Simulation mode")
    parser.add_argument("--csv", help="CSV file path (for single mode)")
    parser.add_argument("--data-dir", default="1분가격", 
                       help="Data directory (for batch mode)")
    parser.add_argument("--scenario", choices=["weekend", "us_closed", "all"], 
                       default="weekend", help="Trading scenario")
    parser.add_argument("--scenarios", nargs="+", 
                       default=["weekend", "us_closed", "all"],
                       help="Scenarios for batch mode")
    parser.add_argument("--output-dir", default="results", 
                       help="Output directory")
    
    # Strategy parameters
    parser.add_argument("--leverage", type=float, default=12.0)
    parser.add_argument("--leg-margin", type=float, default=1000.0)
    parser.add_argument("--price-move-pct", type=float, default=0.0035)
    parser.add_argument("--partial-reduce", type=float, default=0.30)
    parser.add_argument("--atr-factor", type=float, default=0.4)
    parser.add_argument("--preclose-min", type=int, default=45)
    
    args = parser.parse_args()
    
    # Create config
    config = StrategyConfig(
        leverage=args.leverage,
        leg_margin=args.leg_margin,
        price_move_pct=args.price_move_pct,
        partial_reduce=args.partial_reduce,
        atr_factor=args.atr_factor,
        preclose_minutes=args.preclose_min
    )
    
    if args.mode == "single":
        if not args.csv:
            parser.error("--csv is required for single mode")
        run_simulation(args.csv, args.scenario, config, args.output_dir)
    else:
        run_batch_analysis(args.data_dir, args.scenarios, config, args.output_dir)


if __name__ == "__main__":
    main()
