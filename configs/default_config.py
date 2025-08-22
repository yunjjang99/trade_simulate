#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Default configuration for trading strategies
"""

from src.strategies.mean_reversion import StrategyConfig

# Default mean reversion strategy configuration
DEFAULT_CONFIG = StrategyConfig(
    leverage=12.0,
    leg_margin=1000.0,
    price_move_pct=0.0035,  # 0.35%
    partial_reduce=0.30,    # 30%
    atr_factor=0.4,
    preclose_minutes=45,
    immediate_reopen=True
)

# Alternative configurations for testing
CONSERVATIVE_CONFIG = StrategyConfig(
    leverage=8.0,
    leg_margin=1000.0,
    price_move_pct=0.0025,  # 0.25%
    partial_reduce=0.20,    # 20%
    atr_factor=0.3,
    preclose_minutes=60,
    immediate_reopen=True
)

AGGRESSIVE_CONFIG = StrategyConfig(
    leverage=15.0,
    leg_margin=1000.0,
    price_move_pct=0.0050,  # 0.50%
    partial_reduce=0.40,    # 40%
    atr_factor=0.5,
    preclose_minutes=30,
    immediate_reopen=True
)
