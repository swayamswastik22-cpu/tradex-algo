"""Unified Algo Backtesting, Live/Paper Execution, Friction Accounting, and Journal Router.

Endpoints:
- POST /api/algo/backtest: Multi-strategy backtester comparing 4 execution modes side-by-side.
- POST /api/algo/deploy: Deploy strategy in PAPER or double-locked LIVE mode.
- POST /api/algo/arm-live: Layer 1 Live Arming (PIN protected).
- POST /api/algo/disarm-live: Disarm live execution.
- GET /api/algo/status: Current engine and deployment status.
- POST /api/algo/square-off: Hard square-off for all active strategy positions.
- GET /api/journal: Automated trade journal with 2026 friction breakdown and stats.
- POST /api/journal: Add trade entry to journal.
- DELETE /api/journal/clear: Reset journal entries.
"""

import os
import sys
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Query, Body, Request
from pydantic import BaseModel, Field

from services.universal_backtester import run_universal_backtest
from services.algo_execution_engine import algo_engine
from services.trade_journal import trade_journal

router = APIRouter(prefix="/api", tags=["Universal Algo Platform"])


# ==========================================================
# Pydantic Request Models
# ==========================================================
class BacktestRequest(BaseModel):
    symbol: str = Field(default="NIFTY", description="Asset: NIFTY, BANKNIFTY, SENSEX, or any of 218 FNO stocks")
    strategy: str = Field(default="ORB_BREAKOUT", description="Strategy template: ORB_BREAKOUT, VWAP_BOUNCE, SUPERTREND_FLIP, MEAN_REVERSION, EMA_CROSS")
    timeframe: str = Field(default="5m", description="Timeframe: 1m, 3m, 5m, 15m, 30m, 1d")
    entry_start_time: str = Field(default="09:30", description="Avoid opening chop (e.g. 09:30)")
    entry_cutoff_time: str = Field(default="14:45", description="No new entries after this time")
    hard_square_off: str = Field(default="15:00", description="Intraday hard square-off time")
    signal_limit: str = Field(default="FIRST_ONLY", description="FIRST_ONLY, MAX_2, or ALL")
    sl_pct: float = Field(default=0.005, description="Stop loss percent from entry spot (e.g. 0.005 = 0.5%)")
    target_rr: float = Field(default=2.0, description="Risk:Reward ratio for target")
    capital: float = Field(default=1000000.0, description="Starting capital in INR")
    lot_size: Optional[int] = Field(default=None, description="Lot size override")


class DeployRequest(BaseModel):
    strategy: str = Field(default="ORB_BREAKOUT", description="Strategy template name")
    symbol: str = Field(default="NIFTY", description="Symbol")
    mode: str = Field(default="PAPER", description="PAPER or LIVE")
    timeframe: str = Field(default="5m", description="Timeframe")
    sl_pct: float = Field(default=0.005, description="Stop loss percent")
    target_rr: float = Field(default=2.0, description="Risk-reward ratio")
    hard_square_off: str = Field(default="15:00", description="Hard square-off time")
    capital: float = Field(default=100000.0, description="Allocated capital")
    confirmation: Optional[str] = Field(default=None, description="Must be 'TRADE LIVE' if mode is LIVE")


class ArmLiveRequest(BaseModel):
    duration_minutes: int = Field(default=120, description="Arming window duration in minutes")


class JournalAddRequest(BaseModel):
    symbol: str
    instrument: str
    strategy: str = "Manual Desk"
    mode: str = "MANUAL"
    direction: str = "LONG"
    entry_price: float
    exit_price: float
    qty: int = 1
    exit_reason: str = "Exit"
    notes: str = ""


# ==========================================================
# Endpoints
# ==========================================================

@router.post("/algo/backtest", tags=["Algo Platform"])
def algo_backtest_endpoint(req: BacktestRequest):
    """Executes universal backtesting across any of the 218 FNO stocks & 4 indices.

    Compares 4 execution modes (ATM_BUY, DEEP_ITM_BUY, ATM_SELL, FUTURES) side-by-side with 2026 friction accounting.
    """
    from services.day_trader_nse import get_candles

    sym = req.symbol.upper().strip()
    # Fetch historical candles for the symbol
    candle_res = get_candles(symbol=sym, interval=req.timeframe)
    candles = candle_res.get("candles", [])

    if not candles:
        raise HTTPException(status_code=400, detail=f"No candlestick data available for symbol '{sym}'.")

    # Determine default lot size
    lot = req.lot_size
    if not lot:
        if "BANK" in sym:
            lot = 15
        elif "NIFTY" in sym:
            lot = 25
        elif "SENSEX" in sym:
            lot = 10
        else:
            lot = 50

    result = run_universal_backtest(
        candles=candles,
        symbol=sym,
        strategy=req.strategy,
        timeframe=req.timeframe,
        entry_start_time=req.entry_start_time,
        entry_cutoff_time=req.entry_cutoff_time,
        hard_square_off=req.hard_square_off,
        signal_limit=req.signal_limit,
        sl_pct=req.sl_pct,
        target_rr=req.target_rr,
        capital=req.capital,
        lot_size=lot,
    )
    return result


@router.post("/algo/arm-live", tags=["Algo Platform"])
def arm_live_execution(req: ArmLiveRequest = Body(default=ArmLiveRequest())):
    """Layer 1 Live Execution Arming. Must be armed before any LIVE order can be placed."""
    res = algo_engine.arm_live(duration_minutes=req.duration_minutes)
    return res


@router.post("/algo/disarm-live", tags=["Algo Platform"])
def disarm_live_execution():
    """Disarms Layer 1 Live Execution, locking all orders to PAPER mode only."""
    res = algo_engine.disarm_live()
    return res


@router.post("/algo/deploy", tags=["Algo Platform"])
def deploy_algo_strategy(req: DeployRequest):
    """Deploys an active strategy runner. Supports PAPER mode or 2-layer safety locked LIVE mode."""
    try:
        res = algo_engine.deploy_strategy(
            strategy=req.strategy,
            symbol=req.symbol,
            mode=req.mode,
            timeframe=req.timeframe,
            parameters={
                "sl_pct": req.sl_pct,
                "target_rr": req.target_rr,
                "hard_square_off": req.hard_square_off,
            },
            capital=req.capital,
            confirmation=req.confirmation,
        )
        return res
    except ValueError as ve:
        raise HTTPException(status_code=403, detail=str(ve))


@router.get("/algo/status", tags=["Algo Platform"])
def get_algo_status():
    """Returns real-time Algo Engine runtime status, armed state, and active deployments."""
    return algo_engine.get_status()


@router.post("/algo/square-off", tags=["Algo Platform"])
def emergency_square_off():
    """Immediately triggers an intraday square-off on all active strategy positions."""
    return algo_engine.square_off_all()


# ==========================================================
# Trade Journal Endpoints
# ==========================================================

@router.get("/journal", tags=["Trade Journal"])
def get_trade_journal(
    strategy: Optional[str] = Query(default=None, description="Filter by strategy name"),
    mode: Optional[str] = Query(default=None, description="Filter by mode: PAPER, LIVE, MANUAL"),
    symbol: Optional[str] = Query(default=None, description="Filter by symbol"),
):
    """Retrieves full trade journal entries, 2026 friction breakdown, and institutional performance metrics."""
    strat_val = strategy.default if hasattr(strategy, "default") else strategy
    mode_val = mode.default if hasattr(mode, "default") else mode
    sym_val = symbol.default if hasattr(symbol, "default") else symbol
    return trade_journal.get_summary_and_trades(
        strategy_filter=strat_val,
        mode_filter=mode_val,
        symbol_filter=sym_val,
    )


@router.post("/journal", tags=["Trade Journal"])
def add_journal_entry(req: JournalAddRequest):
    """Records a new manual or automated trade entry in the persistent trade journal."""
    res = trade_journal.add_trade(
        symbol=req.symbol,
        instrument=req.instrument,
        strategy=req.strategy,
        mode=req.mode,
        direction=req.direction,
        entry_price=req.entry_price,
        exit_price=req.exit_price,
        qty=req.qty,
        exit_reason=req.exit_reason,
        notes=req.notes,
    )
    return {"status": "success", "trade": res}


@router.delete("/journal/clear", tags=["Trade Journal"])
def clear_trade_journal():
    """Clears all entries in the trade journal (Protected by PIN)."""
    trade_journal.clear()
    return {"status": "success", "message": "Trade journal cleared successfully."}
