#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Output utilities for trading simulation results
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict, Optional
from src.strategies.mean_reversion import StrategyResult


class ResultManager:
    """Manage simulation results and outputs."""
    
    def __init__(self, output_dir: str = "results"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def save_results(self, result: StrategyResult, scenario: str, 
                    month: str, config_name: Optional[str] = None) -> Dict[str, str]:
        """Save simulation results to files."""
        # Create scenario directory
        scenario_dir = self.output_dir / scenario
        scenario_dir.mkdir(exist_ok=True)
        
        # Create month directory
        month_dir = scenario_dir / month
        month_dir.mkdir(exist_ok=True)
        
        # Generate file names
        if config_name:
            base_name = f"{config_name}_{scenario}_{month}"
        else:
            base_name = f"{scenario}_{month}"
        
        events_path = month_dir / f"events_{base_name}.csv"
        equity_path = month_dir / f"equity_{base_name}.csv"
        plot_path = month_dir / f"equity_{base_name}.png"
        summary_path = month_dir / f"summary_{base_name}.txt"
        
        # Save files
        result.events_df.to_csv(events_path, index=False)
        result.equity_df.to_csv(equity_path, index=False)
        
        # Create and save plot
        self._create_equity_plot(result.equity_df, plot_path, f"{scenario} - {month}")
        
        # Save summary
        self._save_summary(result.summary, summary_path, scenario, month)
        
        return {
            "events": str(events_path),
            "equity": str(equity_path),
            "plot": str(plot_path),
            "summary": str(summary_path)
        }
    
    def _create_equity_plot(self, equity_df: pd.DataFrame, plot_path: Path, title: str):
        """Create equity curve plot."""
        plt.figure(figsize=(12, 6))
        if not equity_df.empty:
            plt.plot(equity_df["time"], equity_df["cum_pnl"])
        plt.title(f"Equity Curve - {title}")
        plt.xlabel("Time (UTC)")
        plt.ylabel("Cumulative PnL (USD)")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(plot_path, dpi=130, bbox_inches='tight')
        plt.close()
    
    def _save_summary(self, summary: Dict, summary_path: Path, scenario: str, month: str):
        """Save summary to text file."""
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(f"=== Strategy Summary ===\n")
            f.write(f"Scenario: {scenario}\n")
            f.write(f"Month: {month}\n")
            f.write(f"Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for k, v in summary.items():
                f.write(f"{k}: {v}\n")


def print_summary(result: StrategyResult, scenario: str, month: str):
    """Print summary to console."""
    print(f"\n=== Strategy Summary - {scenario} ({month}) ===")
    for k, v in result.summary.items():
        print(f"{k}: {v}")
