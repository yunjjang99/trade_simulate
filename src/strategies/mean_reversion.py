#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mean Reversion Strategy Implementation
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class StrategyConfig:
    """Configuration for mean reversion strategy."""
    leverage: float = 12.0
    leg_margin: float = 1000.0
    price_move_pct: float = 0.0035
    partial_reduce: float = 0.30
    atr_factor: float = 0.4
    preclose_minutes: int = 45
    immediate_reopen: bool = True


@dataclass
class StrategyResult:
    """Result of strategy simulation."""
    summary: Dict
    equity_df: pd.DataFrame
    events_df: pd.DataFrame


class MeanReversionStrategy:
    """Mean reversion strategy with long/short hedging."""
    
    def __init__(self, config: StrategyConfig):
        self.config = config
    
    def simulate(self, df: pd.DataFrame, base_mask: pd.Series, 
                atr_pct: Optional[pd.Series] = None) -> StrategyResult:
        """Run strategy simulation."""
        if atr_pct is None:
            from src.core.data_loader import calculate_atr
            atr_pct = calculate_atr(df)
        
        allowed = (base_mask) & ((self.config.atr_factor * atr_pct) <= self.config.price_move_pct)
        allowed = allowed.fillna(False)

        # Extract continuous True segments
        segments = self._extract_segments(allowed)
        
        total_pnl = 0.0
        wins = 0
        losses = 0
        preclose_resets = 0
        forced_closes = 0

        equity_times = []
        equity_values = []
        events = []

        for (s, e) in segments:
            seg = df.iloc[s:e+1].copy()
            if seg.empty:
                continue

            seg_end = seg.iloc[-1]["dt_utc"]
            cutoff = seg_end - pd.Timedelta(minutes=self.config.preclose_minutes)

            # Initial state: both legs open
            state = "both_open"
            base_price = float(seg.iloc[0]["open"])
            long_size = 1.0
            short_size = 1.0
            no_new_open = False

            for _, row in seg.iterrows():
                t = row["dt_utc"]
                o = float(row["open"])
                h = float(row["high"])
                l = float(row["low"])
                c = float(row["close"])

                if t >= cutoff:
                    no_new_open = True

                up = base_price * (1.0 + self.config.price_move_pct)
                dn = base_price * (1.0 - self.config.price_move_pct)

                if state == "both_open":
                    hit_up = (h >= up)
                    hit_dn = (l <= dn)
                    
                    if hit_up or hit_dn:
                        # Determine which side hits first
                        if hit_up and hit_dn:
                            first_up = abs(up - o) <= abs(o - dn)
                        else:
                            first_up = hit_up and not hit_dn

                        if first_up:
                            # Long TP profit - Short partial reduction loss
                            pnl_win = self.config.leg_margin * self.config.leverage * self.config.price_move_pct * long_size
                            pnl_loss = self.config.leg_margin * self.config.leverage * self.config.price_move_pct * (self.config.partial_reduce * short_size)
                            net = pnl_win - pnl_loss
                            total_pnl += net
                            
                            if net > 0:
                                wins += 1
                            else:
                                losses += 1
                                
                            short_size *= (1.0 - self.config.partial_reduce)
                            events.append({
                                "time": t, "event": "TP_long", "entry": base_price, "tp_price": up,
                                "pnl_win": round(pnl_win, 6), "pnl_loss": round(-pnl_loss, 6),
                                "net_pnl_change": round(net, 6),
                                "long_size_after": long_size, "short_size_after": short_size,
                                "pnl_cum": round(total_pnl, 6)
                            })
                            
                            state = "short_only" if short_size > 0 else ("flat" if no_new_open else "both_open")
                            if state == "both_open":
                                base_price = o
                                long_size = short_size = 1.0
                                events.append({
                                    "time": t, "event": "instant_reopen", "price": base_price, 
                                    "pnl_cum": round(total_pnl, 6)
                                })
                            equity_times.append(t)
                            equity_values.append(total_pnl)

                        else:
                            # Short TP profit - Long partial reduction loss
                            pnl_win = self.config.leg_margin * self.config.leverage * self.config.price_move_pct * short_size
                            pnl_loss = self.config.leg_margin * self.config.leverage * self.config.price_move_pct * (self.config.partial_reduce * long_size)
                            net = pnl_win - pnl_loss
                            total_pnl += net
                            
                            if net > 0:
                                wins += 1
                            else:
                                losses += 1
                                
                            long_size *= (1.0 - self.config.partial_reduce)
                            events.append({
                                "time": t, "event": "TP_short", "entry": base_price, "tp_price": dn,
                                "pnl_win": round(pnl_win, 6), "pnl_loss": round(-pnl_loss, 6),
                                "net_pnl_change": round(net, 6),
                                "long_size_after": long_size, "short_size_after": short_size,
                                "pnl_cum": round(total_pnl, 6)
                            })
                            
                            state = "long_only" if long_size > 0 else ("flat" if no_new_open else "both_open")
                            if state == "both_open":
                                base_price = o
                                long_size = short_size = 1.0
                                events.append({
                                    "time": t, "event": "instant_reopen", "price": base_price, 
                                    "pnl_cum": round(total_pnl, 6)
                                })
                            equity_times.append(t)
                            equity_values.append(total_pnl)

                # Handle single leg states
                if state == "short_only":
                    if l <= base_price:
                        events.append({
                            "time": t, "event": "revert_close_short", "revert_price": base_price,
                            "pnl_change": 0.0, "pnl_cum": round(total_pnl, 6)
                        })
                        short_size = 0.0
                        if (not no_new_open) and self.config.immediate_reopen:
                            state = "both_open"
                            base_price = o
                            long_size = short_size = 1.0
                            events.append({
                                "time": t, "event": "immediate_reopen", "price": base_price, 
                                "pnl_cum": round(total_pnl, 6)
                            })
                        else:
                            state = "flat"

                elif state == "long_only":
                    if h >= base_price:
                        events.append({
                            "time": t, "event": "revert_close_long", "revert_price": base_price,
                            "pnl_change": 0.0, "pnl_cum": round(total_pnl, 6)
                        })
                        long_size = 0.0
                        if (not no_new_open) and self.config.immediate_reopen:
                            state = "both_open"
                            base_price = o
                            long_size = short_size = 1.0
                            events.append({
                                "time": t, "event": "immediate_reopen", "price": base_price, 
                                "pnl_cum": round(total_pnl, 6)
                            })
                        else:
                            state = "flat"

                # Preclose reset for single legs
                if no_new_open and state in ("short_only", "long_only"):
                    if state == "short_only":
                        pnl_leg = self.config.leg_margin * self.config.leverage * ((base_price - c) / base_price) * short_size
                        total_pnl += pnl_leg
                        if pnl_leg > 0:
                            wins += 1
                        else:
                            losses += 1
                        events.append({
                            "time": t, "event": "preclose_reset_short", "exit": c,
                            "size": short_size, "net_pnl_change": round(pnl_leg, 6), 
                            "pnl_cum": round(total_pnl, 6)
                        })
                        short_size = 0.0
                    else:
                        pnl_leg = self.config.leg_margin * self.config.leverage * ((c - base_price) / base_price) * long_size
                        total_pnl += pnl_leg
                        if pnl_leg > 0:
                            wins += 1
                        else:
                            losses += 1
                        events.append({
                            "time": t, "event": "preclose_reset_long", "exit": c,
                            "size": long_size, "net_pnl_change": round(pnl_leg, 6), 
                            "pnl_cum": round(total_pnl, 6)
                        })
                        long_size = 0.0
                    preclose_resets += 1
                    state = "flat"
                    equity_times.append(t)
                    equity_values.append(total_pnl)

            # Force close remaining positions at segment end
            last = seg.iloc[-1]
            lc = float(last["close"])
            lt = last["dt_utc"]
            
            if state == "short_only" and short_size > 0:
                pnl_leg = self.config.leg_margin * self.config.leverage * ((base_price - lc) / base_price) * short_size
                total_pnl += pnl_leg
                if pnl_leg > 0:
                    wins += 1
                else:
                    losses += 1
                forced_closes += 1
                events.append({
                    "time": lt, "event": "force_close_short", "exit": lc,
                    "size": short_size, "net_pnl_change": round(pnl_leg, 6), 
                    "pnl_cum": round(total_pnl, 6)
                })
                equity_times.append(lt)
                equity_values.append(total_pnl)
            elif state == "long_only" and long_size > 0:
                pnl_leg = self.config.leg_margin * self.config.leverage * ((lc - base_price) / base_price) * long_size
                total_pnl += pnl_leg
                if pnl_leg > 0:
                    wins += 1
                else:
                    losses += 1
                forced_closes += 1
                events.append({
                    "time": lt, "event": "force_close_long", "exit": lc,
                    "size": long_size, "net_pnl_change": round(pnl_leg, 6), 
                    "pnl_cum": round(total_pnl, 6)
                })
                equity_times.append(lt)
                equity_values.append(total_pnl)

        # Build results
        equity_df = pd.DataFrame({"time": equity_times, "cum_pnl": equity_values})
        events_df = pd.DataFrame(events)

        total_events = max(1, wins + losses)
        win_rate = wins / total_events

        summary = {
            "wins_positive_events": wins,
            "loss_negative_events": losses,
            "win_rate": round(win_rate, 4),
            "preclose_resets": preclose_resets,
            "forced_closes": forced_closes,
            "total_realized_pnl_usd": round(total_pnl, 2),
        }
        
        return StrategyResult(summary=summary, equity_df=equity_df, events_df=events_df)
    
    def _extract_segments(self, allowed: pd.Series) -> List[Tuple[int, int]]:
        """Extract continuous True segments from boolean series."""
        segments = []
        idx = allowed.index
        i = 0
        n = len(idx)
        
        while i < n:
            if not allowed.iloc[i]:
                i += 1
                continue
            start = i
            while i < n and allowed.iloc[i]:
                i += 1
            end = i - 1
            segments.append((start, end))
        
        return segments
