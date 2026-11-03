"""Copy Trading Engine & Automated Signal Executor for TRADEx-ALGO.

Features:
1. Auto-Copy Breakout Daemon: Automatically executes paper orders with strict
   1x ATR stop-loss and 1.5R/2.5R target brackets when an ORB breakout triggers.
2. 1-Click Copy Execution: Instantly sizes and copies any selected scanner signal.
3. 1-Click Flatten All: Squares off all open positions immediately.
4. Telegram Notification Dispatch: Broadcasts real-time execution & exit alerts.
"""

import os
import time
import math
import logging
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, Query, Body

from .paper_oms import paper_engine, send_telegram_alert

logger = logging.getLogger("copy_trading_engine")

router = APIRouter(prefix="/api/copy-trade", tags=["Copy Trading & Signal Execution"])

# Global Copy Trading State
class CopyTradingSettings:
    def __init__(self):
        self.lock = threading.Lock()
        self.auto_copy_enabled: bool = False
        self.max_trades_per_day: int = 5
        self.risk_per_trade_pct: float = 1.0  # 1% risk per trade
        self.max_position_capital_pct: float = 20.0  # max 20% capital in single position
        self.copied_symbols_today: set = set()
        self.last_scan_time: str = ""
        self.total_copied_count: int = 0

settings = CopyTradingSettings()


class ToggleAutoCopyRequest(BaseModel):
    enabled: bool
    max_trades_per_day: Optional[int] = 5
    risk_per_trade_pct: Optional[float] = 1.0


class ManualCopyRequest(BaseModel):
    symbol: str = Field(..., example="RELIANCE")
    side: str = Field(default="BUY", example="BUY")
    entry_price: float = Field(..., example=1307.9)
    stop_loss: float = Field(..., example=1300.0)
    target_1: float = Field(..., example=1319.68)
    target_2: Optional[float] = Field(default=None, example=1327.53)
    quantity: Optional[int] = Field(default=None, description="Optional custom quantity; if omitted, automatically sized by risk")


@router.get("/status")
def get_copy_trade_status():
    """Returns the current status of the Copy Trading Engine."""
    with settings.lock:
        open_trades = [t for t in paper_engine.trades.values() if t.status == "OPEN"]
        return {
            "auto_copy_enabled": settings.auto_copy_enabled,
            "max_trades_per_day": settings.max_trades_per_day,
            "risk_per_trade_pct": settings.risk_per_trade_pct,
            "max_position_capital_pct": settings.max_position_capital_pct,
            "copied_symbols_today": list(settings.copied_symbols_today),
            "today_copied_count": len(settings.copied_symbols_today),
            "open_positions_count": len(open_trades),
            "virtual_cash": round(paper_engine.virtual_cash, 2),
            "last_scan_time": settings.last_scan_time,
            "timestamp": datetime.now().isoformat(),
        }


@router.post("/toggle")
def toggle_auto_copy(req: ToggleAutoCopyRequest):
    """Enables or disables background auto-copying of triggered breakouts."""
    with settings.lock:
        settings.auto_copy_enabled = req.enabled
        if req.max_trades_per_day:
            settings.max_trades_per_day = req.max_trades_per_day
        if req.risk_per_trade_pct:
            settings.risk_per_trade_pct = req.risk_per_trade_pct
        status_text = "ENABLED 🟢" if settings.auto_copy_enabled else "DISABLED 🔴"

    msg = f"⚡ *COPY TRADING MODE {status_text}*\nRisk per Trade: {settings.risk_per_trade_pct}%\nMax Trades/Day: {settings.max_trades_per_day}"
    threading.Thread(target=send_telegram_alert, args=(msg,), daemon=True).start()

    return {
        "status": "success",
        "auto_copy_enabled": settings.auto_copy_enabled,
        "message": f"Auto-Copy Trading has been {status_text}.",
    }


@router.post("/execute")
def execute_copy_trade(req: ManualCopyRequest):
    """Executes a 1-click copy trade into the virtual paper OMS."""
    sym = req.symbol.strip().upper()
    entry = round(req.entry_price, 2)
    sl = round(req.stop_loss, 2)
    t1 = round(req.target_1, 2)
    t2 = round(req.target_2, 2) if req.target_2 else round(entry + ((entry - sl) * 2.5), 2)

    risk_per_share = entry - sl
    if risk_per_share <= 0:
        raise HTTPException(status_code=400, detail="Stop loss must be strictly lower than entry price for Long trades.")

    # Calculate lot size
    if req.quantity and req.quantity > 0:
        qty = req.quantity
    else:
        # Auto-size by risk
        wallet = paper_engine.virtual_cash
        max_loss = wallet * (settings.risk_per_trade_pct / 100.0)
        shares_by_risk = math.floor(max_loss / risk_per_share)
        max_pos_cap = wallet * (settings.max_position_capital_pct / 100.0)
        shares_by_cap = math.floor(max_pos_cap / entry) if entry > 0 else 1
        qty = max(1, min(shares_by_risk, shares_by_cap))

    try:
        trade = paper_engine.create_order(
            symbol=sym,
            side=req.side.upper(),
            quantity=qty,
            entry_price=entry,
            stop_loss=sl,
            target_1=t1,
            target_2=t2,
        )
        with settings.lock:
            settings.copied_symbols_today.add(sym)
            settings.total_copied_count += 1

        return {
            "status": "success",
            "message": f"Successfully copied trade for {sym}!",
            "trade": trade.model_dump(),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/flatten-all")
def flatten_all_positions():
    """Squares off all currently open paper positions immediately."""
    closed_list = []
    with paper_engine.lock:
        open_trade_ids = [t.id for t in paper_engine.trades.values() if t.status == "OPEN"]

    for tid in open_trade_ids:
        try:
            closed = paper_engine.close_manually(tid)
            closed_list.append(closed.model_dump())
        except Exception:
            pass

    msg = f"🚨 *FLATTEN ALL EXECUTED*\nClosed {len(closed_list)} open positions.\nTime: {datetime.now().strftime('%H:%M:%S IST')}"
    threading.Thread(target=send_telegram_alert, args=(msg,), daemon=True).start()

    return {
        "status": "success",
        "closed_count": len(closed_list),
        "closed_positions": closed_list,
        "message": f"Successfully squared off {len(closed_list)} positions.",
    }


# ==========================================================
# Background Auto-Copy Worker Loop
# ==========================================================
def _auto_copy_worker_loop():
    """Checks scanner every 5 seconds; auto-copies triggered setups during market hours."""
    while True:
        try:
            time.sleep(5)
            if not settings.auto_copy_enabled:
                continue

            now = datetime.now()
            # Market hours: 09:15 to 15:20 IST
            is_market_active = (
                (now.hour == 9 and now.minute >= 15)
                or (10 <= now.hour < 15)
                or (now.hour == 15 and now.minute <= 20)
            )

            # Flat-by close check: at 15:20 IST auto flatten
            if now.hour == 15 and now.minute >= 20:
                with paper_engine.lock:
                    has_open = any(t.status == "OPEN" for t in paper_engine.trades.values())
                if has_open:
                    flatten_all_positions()
                    continue

            with settings.lock:
                settings.last_scan_time = now.isoformat()
                if len(settings.copied_symbols_today) >= settings.max_trades_per_day:
                    continue

            # Query the scanner
            import sys
            main_mod = sys.modules.get("main")
            if not main_mod:
                continue

            from .day_trader_nse import run_scanner
            scan_res = run_scanner(strategy="orb")
            results = scan_res.get("results", [])

            for item in results:
                sym = item.get("symbol")
                status = item.get("status")

                if status == "triggered" and sym not in settings.copied_symbols_today:
                    entry = item.get("current_price") or item.get("trigger_price")
                    sl = item.get("stop_loss")
                    t1 = item.get("target_1")
                    t2 = item.get("target_2")

                    if entry and sl and t1 and entry > sl:
                        execute_copy_trade(ManualCopyRequest(
                            symbol=sym,
                            side="BUY",
                            entry_price=entry,
                            stop_loss=sl,
                            target_1=t1,
                            target_2=t2,
                        ))
                        logger.info(f"[Auto-Copy] Executed trade for {sym} at Rs {entry}")
                        time.sleep(1)  # small spacing between executions
        except Exception as e:
            logger.warning(f"Error in auto-copy worker loop: {e}")


# Launch background worker thread
threading.Thread(target=_auto_copy_worker_loop, daemon=True, name="AutoCopyWorker").start()
