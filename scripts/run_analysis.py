#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analysis runner script for 2025 data
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from main import run_batch_analysis
from configs.default_config import DEFAULT_CONFIG, CONSERVATIVE_CONFIG, AGGRESSIVE_CONFIG


def run_2025_analysis():
    """Run analysis for 2025 data with different configurations."""
    
    configs = {
        "default": DEFAULT_CONFIG,
        "conservative": CONSERVATIVE_CONFIG,
        "aggressive": AGGRESSIVE_CONFIG
    }
    
    scenarios = ["weekend", "us_closed", "all"]
    
    for config_name, config in configs.items():
        print(f"\n{'='*60}")
        print(f"Running analysis with {config_name} configuration")
        print(f"{'='*60}")
        
        output_dir = f"results/{config_name}"
        run_batch_analysis("1분가격", scenarios, config, output_dir)


if __name__ == "__main__":
    run_2025_analysis()
