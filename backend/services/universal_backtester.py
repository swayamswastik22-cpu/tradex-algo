"""Universal Multi-Strategy Backtesting & 4-Mode Execution Matrix Engine.

Supports 5 strategy templates:
1. ORB_BREAKOUT (15-min Opening Range Breakout)
2. VWAP_BOUNCE (Intraday Institutional Volume-Weighted Average Price Bounce)
3. SUPERTREND_FLIP (Trend-following ATR Trailing Stop Reversal)
4. EMA_CROSS (9/21 Exponential Moving Average Dynamic Trend Cross)
5. MEAN_REVERSION (Bollinger/ATR Dynamic Exhaustion Reversal)

Evaluates and compares 4 execution modes side-by-side:
- ATM_BUY (Long ATM Call / Put)
- DEEP_ITM_BUY (High-Delta ~0.80 In-the-Money Option)
- ATM_SELL (Short ATM Put / Call credit spread)
- FUTURES (1:1 Linear Delta Contract)

Includes Whipsaw Trade Detection and 2026 Indian Friction Accounting.
"""

import math
from datetime import datetime, date, time as dtime
from typing import Dict, Any, List, Optional, Tuple

from services.friction_engine import calculate_trade_friction


def compute_indicators_series(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Computes technical indicator arrays (VWAP, EMA Fast, EMA Slow, Supertrend, ORB High/Low) across candles."""
    n = len(candles)
    if n == 0:
        return {
            "vwap": [],
            "ema_fast": [],
            "ema_slow": [],
            "supertrend": [],
            "orb_high": None,
            "orb_low": None,
        }

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c.get("volume", 1000) for c in candles]

    # 1. VWAP (Cumulative Price*Volume / Cumulative Volume)
    vwap = []
    cum_pv = 0.0
    cum_vol = 0.0
    for h, l, c, v in zip(highs, lows, closes, volumes):
        typical_p = (h + l + c) / 3.0
        v_clean = max(v, 1)
        cum_pv += typical_p * v_clean
        cum_vol += v_clean
        vwap.append(round(cum_pv / cum_vol, 2))

    # 2. EMAs (9 and 21)
    def calc_ema(series: List[float], period: int) -> List[float]:
        res = []
        multiplier = 2.0 / (period + 1.0)
        curr = series[0] if series else 0.0
        for val in series:
            curr = (val - curr) * multiplier + curr
            res.append(round(curr, 2))
        return res

    ema_fast = calc_ema(closes, 9)
    ema_slow = calc_ema(closes, 21)

    # 3. Supertrend (10, 3)
    supertrend = []
    atr_period = 10
    multiplier = 3.0
    tr_list = []
    for i in range(n):
        if i == 0:
            tr_list.append(highs[i] - lows[i])
        else:
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i - 1])
            lc = abs(lows[i] - closes[i - 1])
            tr_list.append(max(hl, hc, lc))

    # Rolling ATR
    atr = []
    curr_atr = sum(tr_list[:atr_period]) / float(max(atr_period, 1)) if n >= atr_period else (tr_list[0] if tr_list else 1.0)
    for i in range(n):
        if i < atr_period:
            atr.append(curr_atr)
        else:
            curr_atr = (curr_atr * (atr_period - 1) + tr_list[i]) / atr_period
            atr.append(curr_atr)

    trend = 1
    upper_band = 0.0
    lower_band = 0.0
    for i in range(n):
        hl2 = (highs[i] + lows[i]) / 2.0
        basic_upper = hl2 + multiplier * atr[i]
        basic_lower = hl2 - multiplier * atr[i]

        if i == 0:
            upper_band = basic_upper
            lower_band = basic_lower
        else:
            if basic_upper < upper_band or closes[i - 1] > upper_band:
                upper_band = basic_upper
            if basic_lower > lower_band or closes[i - 1] < lower_band:
                lower_band = basic_lower

        if closes[i] > upper_band:
            trend = 1
        elif closes[i] < lower_band:
            trend = -1

        st_val = lower_band if trend == 1 else upper_band
        supertrend.append({
            "value": round(st_val, 2),
            "trend": "bullish" if trend == 1 else "bearish",
            "upper": round(upper_band, 2),
            "lower": round(lower_band, 2),
        })

    # 4. ORB High and Low (First 15 mins = first 3 bars of 5m or 15 bars of 1m)
    orb_bars = min(n, 3)
    orb_high = max(highs[:orb_bars]) if orb_bars > 0 else highs[0]
    orb_low = min(lows[:orb_bars]) if orb_bars > 0 else lows[0]

    return {
        "vwap": vwap,
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
        "supertrend": supertrend,
        "orb_high": round(orb_high, 2),
        "orb_low": round(orb_low, 2),
    }


def run_universal_backtest(
    candles: List[Dict[str, Any]],
    symbol: str = "NIFTY",
    strategy: str = "ORB_BREAKOUT",
    timeframe: str = "5m",
    entry_start_time: str = "09:30",
    entry_cutoff_time: str = "14:45",
    hard_square_off: str = "15:00",
    signal_limit: str = "FIRST_ONLY",
    sl_pct: float = 0.005,
    target_rr: float = 2.0,
    capital: float = 1000000.0,
    lot_size: int = 25,
) -> Dict[str, Any]:
    """Runs a backtest across the candle series and computes side-by-side performance for 4 execution modes."""
    if not candles:
        return {"error": "No candle data available for backtest."}

    indicators = compute_indicators_series(candles)
    vwap = indicators["vwap"]
    ema_f = indicators["ema_fast"]
    ema_s = indicators["ema_slow"]
    supertrend = indicators["supertrend"]
    orb_high = indicators["orb_high"]
    orb_low = indicators["orb_low"]

    strat = strategy.upper().strip()
    sig_limit = signal_limit.upper().strip()

    # Parse cutoff times
    def parse_time_str(ts_str: str) -> Tuple[int, int]:
        try:
            parts = ts_str.split(":")
            return int(parts[0]), int(parts[1])
        except Exception:
            return 9, 30

    start_h, start_m = parse_time_str(entry_start_time)
    cutoff_h, cutoff_m = parse_time_str(entry_cutoff_time)
    sqoff_h, sqoff_m = parse_time_str(hard_square_off)

    raw_signals: List[Dict[str, Any]] = []

    # 1. Generate Raw Strategy Signals
    for i in range(1, len(candles)):
        c = candles[i]
        t_str = c.get("time_str", "10:00")
        try:
            # Check time constraints
            parts = t_str.split(" ")[-1].split(":")
            chour, cmin = int(parts[0]), int(parts[1])
            t_val = chour * 60 + cmin
            t_start = start_h * 60 + start_m
            t_cutoff = cutoff_h * 60 + cutoff_m
            t_sqoff = sqoff_h * 60 + sqoff_m
        except Exception:
            t_val = 600
            t_start, t_cutoff, t_sqoff = 570, 885, 900

        if t_val < t_start or t_val > t_cutoff:
            continue

        direction = None
        reason = ""

        if strat == "ORB_BREAKOUT":
            if orb_high and c["close"] > orb_high and candles[i - 1]["close"] <= orb_high:
                direction = "LONG"
                reason = "ORB Breakout High"
            elif orb_low and c["close"] < orb_low and candles[i - 1]["close"] >= orb_low:
                direction = "SHORT"
                reason = "ORB Breakdown Low"

        elif strat == "VWAP_BOUNCE":
            curr_vwap = vwap[i]
            prev_vwap = vwap[i - 1]
            if candles[i - 1]["low"] <= prev_vwap and c["close"] > curr_vwap:
                direction = "LONG"
                reason = "VWAP Bullish Bounce"
            elif candles[i - 1]["high"] >= prev_vwap and c["close"] < curr_vwap:
                direction = "SHORT"
                reason = "VWAP Bearish Rejection"

        elif strat == "SUPERTREND_FLIP":
            if supertrend[i]["trend"] == "bullish" and supertrend[i - 1]["trend"] == "bearish":
                direction = "LONG"
                reason = "Supertrend Flipped Green"
            elif supertrend[i]["trend"] == "bearish" and supertrend[i - 1]["trend"] == "bullish":
                direction = "SHORT"
                reason = "Supertrend Flipped Red"

        elif strat == "EMA_CROSS":
            if ema_f[i] > ema_s[i] and ema_f[i - 1] <= ema_s[i - 1]:
                direction = "LONG"
                reason = "9 EMA Crossed Above 21 EMA"
            elif ema_f[i] < ema_s[i] and ema_f[i - 1] >= ema_s[i - 1]:
                direction = "SHORT"
                reason = "9 EMA Crossed Below 21 EMA"

        elif strat == "MEAN_REVERSION":
            curr_vwap = vwap[i]
            dev = abs(c["close"] - curr_vwap) / curr_vwap
            if dev > 0.008 and c["close"] < curr_vwap and c["close"] > c["open"]:
                direction = "LONG"
                reason = "Oversold Mean Reversion"
            elif dev > 0.008 and c["close"] > curr_vwap and c["close"] < c["open"]:
                direction = "SHORT"
                reason = "Overbought Mean Reversion"

        if direction:
            raw_signals.append({
                "bar_index": i,
                "timestamp": c["time"],
                "time_str": t_str,
                "direction": direction,
                "entry_price": c["close"],
                "reason": reason,
            })

    # Apply signal limit filter
    active_signals = []
    if sig_limit == "FIRST_ONLY" and raw_signals:
        active_signals = [raw_signals[0]]
    elif sig_limit == "MAX_2" and raw_signals:
        active_signals = raw_signals[:2]
    else:
        active_signals = raw_signals

    # 2. Simulate Trades across 4 Execution Modes
    # Modes: ATM_BUY, DEEP_ITM_BUY, ATM_SELL, FUTURES
    modes = ["ATM_BUY", "DEEP_ITM_BUY", "ATM_SELL", "FUTURES"]
    results_by_mode: Dict[str, Any] = {}

    for mode in modes:
        mode_trades = []
        whipsaw_count = 0
        markers: List[Dict[str, Any]] = []

        for sig in active_signals:
            entry_idx = sig["bar_index"]
            spot_entry = sig["entry_price"]
            direction = sig["direction"]

            # Calculate option premium or futures pricing
            # ATM premium is approx 1.2% of spot for indices
            atm_prem = round(spot_entry * 0.012, 2)
            deep_itm_prem = round(spot_entry * 0.025, 2)
            atm_credit = atm_prem

            if mode == "ATM_BUY":
                inst_type = "OPTION"
                exec_entry = atm_prem
                delta = 0.52
            elif mode == "DEEP_ITM_BUY":
                inst_type = "OPTION"
                exec_entry = deep_itm_prem
                delta = 0.82
            elif mode == "ATM_SELL":
                inst_type = "OPTION"
                exec_entry = atm_credit
                delta = -0.50
            else:  # FUTURES
                inst_type = "FUTURES"
                exec_entry = spot_entry
                delta = 1.0

            stop_distance = spot_entry * sl_pct
            target_distance = stop_distance * target_rr

            sl_spot = spot_entry - stop_distance if direction == "LONG" else spot_entry + stop_distance
            tp_spot = spot_entry + target_distance if direction == "LONG" else spot_entry - target_distance

            exit_idx = None
            exit_spot = None
            exit_reason = ""
            bars_held = 0

            # Step forward bar by bar
            for j in range(entry_idx + 1, len(candles)):
                bar = candles[j]
                bars_held += 1

                # Check time for hard square-off
                t_str = bar.get("time_str", "15:00")
                try:
                    parts = t_str.split(" ")[-1].split(":")
                    b_hour, b_min = int(parts[0]), int(parts[1])
                    b_val = b_hour * 60 + b_min
                    t_sqoff = sqoff_h * 60 + sqoff_m
                except Exception:
                    b_val = 900
                    t_sqoff = 900

                # Check Target & Stop Loss hits
                if direction == "LONG":
                    if bar["low"] <= sl_spot:
                        exit_idx = j
                        exit_spot = sl_spot
                        exit_reason = "SL Hit"
                        break
                    elif bar["high"] >= tp_spot:
                        exit_idx = j
                        exit_spot = tp_spot
                        exit_reason = "Target Hit"
                        break
                else:  # SHORT
                    if bar["high"] >= sl_spot:
                        exit_idx = j
                        exit_spot = sl_spot
                        exit_reason = "SL Hit"
                        break
                    elif bar["low"] <= tp_spot:
                        exit_idx = j
                        exit_spot = tp_spot
                        exit_reason = "Target Hit"
                        break

                if b_val >= t_sqoff:
                    exit_idx = j
                    exit_spot = bar["close"]
                    exit_reason = f"Hard Square-Off ({hard_square_off})"
                    break

            if exit_idx is None:
                exit_idx = len(candles) - 1
                exit_spot = candles[-1]["close"]
                exit_reason = "Session End"

            # Check Whipsaw
            is_whipsaw = False
            if bars_held <= 2 and exit_reason == "SL Hit":
                is_whipsaw = True
                whipsaw_count += 1

            # Translate spot movement to execution vehicle PnL
            spot_diff = (exit_spot - spot_entry) if direction == "LONG" else (spot_entry - exit_spot)
            
            if mode in ["ATM_BUY", "DEEP_ITM_BUY"]:
                exec_exit = max(round(exec_entry + (spot_diff * delta), 2), 0.05)
                friction_res = calculate_trade_friction(
                    instrument_type="OPTION",
                    entry_price=exec_entry,
                    exit_price=exec_exit,
                    qty=lot_size,
                    is_buy_first=True,
                )
            elif mode == "ATM_SELL":
                # Option seller profits when spot_diff is favorable (premium decays)
                exec_exit = max(round(exec_entry - (spot_diff * abs(delta)), 2), 0.05)
                friction_res = calculate_trade_friction(
                    instrument_type="OPTION",
                    entry_price=exec_entry,
                    exit_price=exec_exit,
                    qty=lot_size,
                    is_buy_first=False,
                )
            else:  # FUTURES
                exec_exit = exit_spot
                friction_res = calculate_trade_friction(
                    instrument_type="FUTURES",
                    entry_price=exec_entry,
                    exit_price=exec_exit,
                    qty=lot_size,
                    is_buy_first=(direction == "LONG"),
                )

            trade_record = {
                "trade_num": len(mode_trades) + 1,
                "direction": direction,
                "strategy": strat,
                "mode": mode,
                "entry_time": sig["time_str"],
                "entry_timestamp": sig["timestamp"],
                "entry_spot": round(spot_entry, 2),
                "exec_entry_price": exec_entry,
                "exit_time": candles[exit_idx]["time_str"],
                "exit_timestamp": candles[exit_idx]["time"],
                "exit_spot": round(exit_spot, 2),
                "exec_exit_price": exec_exit,
                "bars_held": bars_held,
                "exit_reason": exit_reason,
                "is_whipsaw": is_whipsaw,
                "qty": lot_size,
                "gross_pnl": friction_res["gross_pnl"],
                "friction": friction_res["total_friction"],
                "net_pnl": friction_res["net_pnl"],
                "friction_breakdown": friction_res["breakdown"],
            }
            mode_trades.append(trade_record)

            # Build TradingView Markers
            markers.append({
                "time": sig["timestamp"],
                "position": "belowBar" if direction == "LONG" else "aboveBar",
                "color": "#10b981" if direction == "LONG" else "#ef4444",
                "shape": "arrowUp" if direction == "LONG" else "arrowDown",
                "text": f"{direction} ({mode}) @ {spot_entry}",
            })
            marker_color = "#10b981" if friction_res["net_pnl"] > 0 else "#ef4444"
            if "Square-Off" in exit_reason:
                marker_color = "#8b5cf6"
            markers.append({
                "time": candles[exit_idx]["time"],
                "position": "aboveBar" if direction == "LONG" else "belowBar",
                "color": marker_color,
                "shape": "circle",
                "text": f"{exit_reason} (Net: Rs {friction_res['net_pnl']})",
            })

        # Calculate Summary Metrics for Mode
        total_trades = len(mode_trades)
        wins = [t for t in mode_trades if t["net_pnl"] > 0]
        losses = [t for t in mode_trades if t["net_pnl"] <= 0]
        win_rate = round((len(wins) / total_trades) * 100, 2) if total_trades > 0 else 0.0

        gross_pnl = round(sum(t["gross_pnl"] for t in mode_trades), 2)
        total_friction = round(sum(t["friction"] for t in mode_trades), 2)
        net_pnl = round(sum(t["net_pnl"] for t in mode_trades), 2)

        gross_wins = sum(t["gross_pnl"] for t in wins)
        gross_losses = abs(sum(t["gross_pnl"] for t in losses))
        profit_factor = round(gross_wins / max(gross_losses, 1.0), 2) if total_trades > 0 else 0.0
        expectancy = round(net_pnl / max(total_trades, 1), 2)

        # Max Drawdown
        cum_pnl = 0.0
        peak = 0.0
        max_dd = 0.0
        for t in mode_trades:
            cum_pnl += t["net_pnl"]
            if cum_pnl > peak:
                peak = cum_pnl
            dd = peak - cum_pnl
            if dd > max_dd:
                max_dd = dd

        results_by_mode[mode] = {
            "mode": mode,
            "total_trades": total_trades,
            "winning_trades": len(wins),
            "losing_trades": len(losses),
            "win_rate": win_rate,
            "gross_pnl": gross_pnl,
            "total_friction": total_friction,
            "net_pnl": net_pnl,
            "profit_factor": profit_factor,
            "expectancy_per_trade": expectancy,
            "max_drawdown": round(max_dd, 2),
            "whipsaw_count": whipsaw_count,
            "whipsaw_rate": round((whipsaw_count / max(total_trades, 1)) * 100, 2),
            "trades": mode_trades,
            "markers": markers,
        }

    # Best mode by Net P&L
    best_mode = max(results_by_mode.keys(), key=lambda m: results_by_mode[m]["net_pnl"])

    return {
        "status": "success",
        "symbol": symbol,
        "strategy": strat,
        "timeframe": timeframe,
        "capital": capital,
        "lot_size": lot_size,
        "parameters": {
            "entry_start_time": entry_start_time,
            "entry_cutoff_time": entry_cutoff_time,
            "hard_square_off": hard_square_off,
            "signal_limit": sig_limit,
            "sl_pct": sl_pct,
            "target_rr": target_rr,
        },
        "best_execution_mode": best_mode,
        "total_trades": results_by_mode[best_mode]["total_trades"],
        "win_rate": results_by_mode[best_mode]["win_rate"],
        "gross_pnl": results_by_mode[best_mode]["gross_pnl"],
        "total_friction": results_by_mode[best_mode]["total_friction"],
        "net_pnl": results_by_mode[best_mode]["net_pnl"],
        "expectancy_per_trade": results_by_mode[best_mode]["expectancy_per_trade"],
        "profit_factor": results_by_mode[best_mode]["profit_factor"],
        "max_drawdown": results_by_mode[best_mode]["max_drawdown"],
        "whipsaw_count": results_by_mode[best_mode]["whipsaw_count"],
        "trades": results_by_mode[best_mode]["trades"],
        "markers": results_by_mode[best_mode]["markers"],
        "summary": results_by_mode[best_mode],
        "mode_comparison": {
            m: {
                "win_rate": results_by_mode[m]["win_rate"],
                "gross_pnl": results_by_mode[m]["gross_pnl"],
                "total_friction": results_by_mode[m]["total_friction"],
                "net_pnl": results_by_mode[m]["net_pnl"],
                "profit_factor": results_by_mode[m]["profit_factor"],
                "expectancy": results_by_mode[m]["expectancy_per_trade"],
                "max_drawdown": results_by_mode[m]["max_drawdown"],
                "whipsaw_count": results_by_mode[m]["whipsaw_count"],
            }
            for m in modes
        },
        "all_modes_data": results_by_mode,
        "indicators": {
            "orb_high": orb_high,
            "orb_low": orb_low,
            "latest_vwap": vwap[-1] if vwap else None,
            "latest_ema_fast": ema_f[-1] if ema_f else None,
            "latest_ema_slow": ema_s[-1] if ema_s else None,
            "latest_supertrend": supertrend[-1] if supertrend else None,
        },
        "tradingview_markers": results_by_mode[best_mode]["markers"],
    }
