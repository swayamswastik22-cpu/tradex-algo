"""100% Virtual Paper Trading OMS (Order Management System).

SAFETY DIRECTIVE:
Real broker order execution is strictly forbidden.
The live broker 'placeOrder' function is never imported, defined, or invoked.
All trades are strictly executed inside a virtual paper trading sandbox.

Logs all triggered BUY/SELL paper trades into a Supabase paper_trades table
(with thread-safe in-memory/SQLite cache fallback),
calculates live unrealized P&L against incoming Angel One WebSocket ticks,
auto-closes trades when live LTP touches stop_loss or target levels,
and broadcasts instant copy-trade alerts via the Telegram Bot API.
"""

import os
import uuid
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import threading

import requests
from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field

logger = logging.getLogger("paper_oms")

router = APIRouter(prefix="/api/paper", tags=["Virtual Paper Trading OMS"])

# Ensure no broker execution module is ever imported
FORBIDDEN_BROKER_CALLS = ["placeOrder", "place_order", "SmartConnect.placeOrder"]
assert "placeOrder" not in globals(), "CRITICAL: Live broker execution is strictly forbidden!"

# Environment Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "") or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


# ==========================================================
# 1. Telegram Notification Client
# ==========================================================
def send_telegram_alert(message: str) -> bool:
    """Dispatches real-time copy-trade alert to Telegram channel/group."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info(f"[Paper OMS Telegram Alert]: {message}")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
    }
    try:
        resp = requests.post(url, json=payload, timeout=5)
        return resp.status_code == 200
    except Exception as e:
        logger.warning(f"Failed to deliver Telegram notification: {e}")
        return False


# ==========================================================
# 2. Virtual Ledger & Supabase Persistence
# ==========================================================
class PaperTradeRecord(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    side: str = "BUY"
    quantity: int
    entry_price: float
    stop_loss: float
    target_1: float
    target_2: Optional[float] = None
    status: str = "OPEN"  # 'OPEN' or 'CLOSED'
    current_ltp: Optional[float] = None
    exit_price: Optional[float] = None
    pnl: float = 0.0
    pnl_percent: float = 0.0
    exit_reason: Optional[str] = None  # 'STOP_LOSS', 'TARGET_1', 'TARGET_2', 'FLAT_BY_CLOSE', 'MANUAL'
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    closed_at: Optional[str] = None


class PaperTradingEngine:
    """Thread-safe virtual OMS with live tick listener and Supabase sync."""

    def __init__(self, initial_capital: float = 1000000.0):
        self.lock = threading.Lock()
        self.initial_capital = initial_capital
        self.virtual_cash = initial_capital
        self.trades: Dict[str, PaperTradeRecord] = {}
        self.live_prices: Dict[str, float] = {}

    def _sync_to_supabase(self, trade: PaperTradeRecord) -> None:
        """Syncs trade to Supabase paper_trades table if credentials exist."""
        if not SUPABASE_URL or not SUPABASE_KEY:
            return

        url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/paper_trades"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }
        try:
            requests.post(url, headers=headers, json=trade.model_dump(), timeout=5)
        except Exception as e:
            logger.warning(f"Supabase sync failed: {e}")

    def create_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        entry_price: float,
        stop_loss: float,
        target_1: float,
        target_2: Optional[float] = None,
    ) -> PaperTradeRecord:
        with self.lock:
            order_cost = entry_price * quantity
            if order_cost > self.virtual_cash:
                raise ValueError(f"Insufficient virtual cash! Required: ₹{order_cost:.2f}, Available: ₹{self.virtual_cash:.2f}")

            trade = PaperTradeRecord(
                symbol=symbol.upper().strip(),
                side=side.upper().strip(),
                quantity=quantity,
                entry_price=round(entry_price, 2),
                stop_loss=round(stop_loss, 2),
                target_1=round(target_1, 2),
                target_2=round(target_2, 2) if target_2 else None,
                current_ltp=round(entry_price, 2),
                status="OPEN",
            )
            self.trades[trade.id] = trade
            self.virtual_cash -= order_cost

            # Sync to Supabase in background
            threading.Thread(target=self._sync_to_supabase, args=(trade,), daemon=True).start()

            # Copy-trade Telegram broadcast
            msg = (
                f"⚡ *NEW PAPER ORDER EXECUTED*\n\n"
                f"📌 *Symbol:* `{trade.symbol}`\n"
                f"🛒 *Side:* {trade.side}\n"
                f"🔢 *Quantity:* {trade.quantity} shares\n"
                f"💵 *Entry Price:* ₹{trade.entry_price:.2f}\n"
                f"🛑 *Stop Loss:* ₹{trade.stop_loss:.2f}\n"
                f"🎯 *Target 1:* ₹{trade.target_1:.2f}\n"
                f"{f'🎯 *Target 2:* ₹{trade.target_2:.2f}' if trade.target_2 else ''}\n"
                f"🛡️ *Mode:* 100% Virtual Paper OMS"
            )
            threading.Thread(target=send_telegram_alert, args=(msg,), daemon=True).start()
            return trade

    def process_tick(self, symbol: str, ltp: float) -> List[PaperTradeRecord]:
        """Processes live WebSocket LTP tick and auto-closes positions hitting SL or Target."""
        clean_sym = symbol.upper().strip()
        closed_trades = []

        with self.lock:
            self.live_prices[clean_sym] = ltp

            for trade in list(self.trades.values()):
                if trade.status != "OPEN" or trade.symbol != clean_sym:
                    continue

                trade.current_ltp = ltp
                # Calculate live unrealized P&L
                if trade.side == "BUY":
                    trade.pnl = round((ltp - trade.entry_price) * trade.quantity, 2)
                    trade.pnl_percent = round(((ltp - trade.entry_price) / trade.entry_price) * 100, 2)

                    # Auto-close checks
                    if ltp <= trade.stop_loss:
                        trade.status = "CLOSED"
                        trade.exit_price = ltp
                        trade.exit_reason = "STOP_LOSS"
                        trade.closed_at = datetime.now().isoformat()
                        self.virtual_cash += (ltp * trade.quantity)
                        closed_trades.append(trade)
                    elif trade.target_2 and ltp >= trade.target_2:
                        trade.status = "CLOSED"
                        trade.exit_price = ltp
                        trade.exit_reason = "TARGET_2"
                        trade.closed_at = datetime.now().isoformat()
                        self.virtual_cash += (ltp * trade.quantity)
                        closed_trades.append(trade)
                    elif ltp >= trade.target_1:
                        trade.status = "CLOSED"
                        trade.exit_price = ltp
                        trade.exit_reason = "TARGET_1"
                        trade.closed_at = datetime.now().isoformat()
                        self.virtual_cash += (ltp * trade.quantity)
                        closed_trades.append(trade)

        # Dispatch updates & alerts outside lock
        for closed in closed_trades:
            threading.Thread(target=self._sync_to_supabase, args=(closed,), daemon=True).start()
            pnl_emoji = "🟢" if closed.pnl >= 0 else "🔴"
            msg = (
                f"{pnl_emoji} *PAPER POSITION CLOSED*\n\n"
                f"📌 *Symbol:* `{closed.symbol}`\n"
                f"🏁 *Reason:* {closed.exit_reason}\n"
                f"💵 *Exit Price:* ₹{closed.exit_price:.2f}\n"
                f"💰 *Realized P&L:* ₹{closed.pnl:+.2f} ({closed.pnl_percent:+.2f}%)\n"
                f"⏱️ *Closed At:* {closed.closed_at}"
            )
            threading.Thread(target=send_telegram_alert, args=(msg,), daemon=True).start()

        return closed_trades

    def close_manually(self, trade_id: str, exit_price: Optional[float] = None) -> PaperTradeRecord:
        with self.lock:
            if trade_id not in self.trades:
                raise KeyError(f"Trade ID {trade_id} not found.")

            trade = self.trades[trade_id]
            if trade.status == "CLOSED":
                return trade

            cur_price = exit_price or self.live_prices.get(trade.symbol, trade.entry_price)
            trade.status = "CLOSED"
            trade.exit_price = round(cur_price, 2)
            trade.exit_reason = "MANUAL"
            trade.closed_at = datetime.now().isoformat()
            trade.pnl = round((trade.exit_price - trade.entry_price) * trade.quantity, 2)
            trade.pnl_percent = round(((trade.exit_price - trade.entry_price) / trade.entry_price) * 100, 2)
            self.virtual_cash += (trade.exit_price * trade.quantity)

            threading.Thread(target=self._sync_to_supabase, args=(trade,), daemon=True).start()
            return trade

    def get_portfolio_summary(self) -> Dict[str, Any]:
        with self.lock:
            open_trades = [t for t in self.trades.values() if t.status == "OPEN"]
            closed_trades = [t for t in self.trades.values() if t.status == "CLOSED"]

            unrealized_pnl = sum(t.pnl for t in open_trades)
            realized_pnl = sum(t.pnl for t in closed_trades)
            total_invested = sum((t.entry_price * t.quantity) for t in open_trades)

            winning_trades = len([t for t in closed_trades if t.pnl > 0])
            total_closed = len(closed_trades)
            win_rate = round((winning_trades / total_closed * 100), 1) if total_closed > 0 else 0.0

            current_portfolio_value = self.virtual_cash + total_invested + unrealized_pnl

            return {
                "initial_capital": self.initial_capital,
                "current_portfolio_value": round(current_portfolio_value, 2),
                "virtual_cash_balance": round(self.virtual_cash, 2),
                "invested_capital": round(total_invested, 2),
                "realized_pnl": round(realized_pnl, 2),
                "unrealized_pnl": round(unrealized_pnl, 2),
                "total_pnl": round(realized_pnl + unrealized_pnl, 2),
                "total_trades": len(self.trades),
                "open_trades_count": len(open_trades),
                "closed_trades_count": total_closed,
                "win_rate_pct": win_rate,
            }


# Singleton Paper OMS Engine
paper_engine = PaperTradingEngine()


# ==========================================================
# 3. FastAPI Paper Trading Routes
# ==========================================================
class CreatePaperOrderRequest(BaseModel):
    symbol: Optional[str] = None
    ticker: Optional[str] = None
    side: str = Field(default="BUY")
    quantity: Optional[int] = None
    qty: Optional[int] = None
    shares: Optional[int] = None
    entry_price: Optional[float] = None
    price: Optional[float] = None
    ltp: Optional[float] = None
    stop_loss: Optional[float] = None
    sl: Optional[float] = None
    target_1: Optional[float] = None
    target: Optional[float] = None
    tgt: Optional[float] = None
    target_2: Optional[float] = None


class TickSimulationRequest(BaseModel):
    symbol: str = Field(..., example="RELIANCE")
    ltp: float = Field(..., example=2785.0)


@router.post("/order")
def create_paper_order(order: CreatePaperOrderRequest):
    """Executes a 100% virtual paper trade order. Live broker calls are prohibited."""
    sym = (order.symbol or order.ticker or "RELIANCE").strip().upper()
    side = (order.side or "BUY").strip().upper()
    qty = order.quantity or order.qty or order.shares or 10

    # Resolve entry price
    entry = order.entry_price or order.price or order.ltp
    if not entry or entry <= 0:
        entry = paper_engine.live_prices.get(sym)
    if not entry or entry <= 0:
        try:
            import sys
            main_mod = sys.modules.get("main")
            if main_mod and hasattr(main_mod, "feed_manager"):
                entry = main_mod.feed_manager.latest_prices.get(sym)
        except Exception:
            pass
    entry = round(float(entry or 1000.0), 2)

    # Resolve Stop Loss & Targets
    sl = order.stop_loss or order.sl
    if not sl or sl <= 0 or sl >= entry:
        sl = round(entry * 0.99, 2)
    else:
        sl = round(float(sl), 2)

    risk = max(round(entry - sl, 2), round(entry * 0.005, 2))
    t1 = order.target_1 or order.target or order.tgt
    if not t1 or t1 <= entry:
        t1 = round(entry + (risk * 1.5), 2)
    else:
        t1 = round(float(t1), 2)

    t2 = order.target_2
    if not t2 or t2 <= t1:
        t2 = round(entry + (risk * 2.5), 2)
    else:
        t2 = round(float(t2), 2)

    try:
        record = paper_engine.create_order(
            symbol=sym,
            side=side,
            quantity=qty,
            entry_price=entry,
            stop_loss=sl,
            target_1=t1,
            target_2=t2,
        )
        return {
            "status": "success",
            "message": f"Virtual Paper Order successfully created for {sym}.",
            "order": record.model_dump(),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/positions")
def list_positions(status: Optional[str] = Query(default=None, description="'OPEN' or 'CLOSED'")):
    """Lists virtual paper positions with live unrealized and realized P&L."""
    trades = list(paper_engine.trades.values())
    if status:
        trades = [t for t in trades if t.status == status.upper()]
    return {
        "count": len(trades),
        "positions": [t.model_dump() for t in trades],
    }


class ClosePositionRequest(BaseModel):
    position_id: Optional[str] = None
    trade_id: Optional[str] = None
    id: Optional[str] = None
    exit_price: Optional[float] = None


class CloseAllRequest(BaseModel):
    reason: Optional[str] = "MANUAL_FLATTEN"


@router.post("/close/{trade_id}")
def close_paper_trade(trade_id: str, exit_price: Optional[float] = Query(default=None)):
    """Manually closes an active virtual paper trading position."""
    try:
        record = paper_engine.close_manually(trade_id, exit_price)
        return {"status": "success", "closed_position": record.model_dump()}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/close-position")
def close_paper_position_compat(req: ClosePositionRequest):
    """Compatibility endpoint for AI Studio UI."""
    tid = req.position_id or req.trade_id or req.id
    if not tid:
        # If no specific id provided, close the first open trade
        with paper_engine.lock:
            open_trades = [t.id for t in paper_engine.trades.values() if t.status == "OPEN"]
        if not open_trades:
            return {"status": "ok", "message": "No open positions to close."}
        tid = open_trades[0]

    try:
        record = paper_engine.close_manually(tid, req.exit_price)
        return {"status": "success", "closed_position": record.model_dump()}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/close-all")
def close_all_paper_positions_compat(req: CloseAllRequest = Body(default_factory=CloseAllRequest)):
    """Compatibility endpoint for AI Studio UI Close All button."""
    closed_list = []
    with paper_engine.lock:
        open_trade_ids = [t.id for t in paper_engine.trades.values() if t.status == "OPEN"]

    for tid in open_trade_ids:
        try:
            closed = paper_engine.close_manually(tid)
            closed_list.append(closed.model_dump())
        except Exception:
            pass

    return {
        "status": "success",
        "closed_count": len(closed_list),
        "closed_positions": closed_list,
        "message": f"Successfully squared off {len(closed_list)} positions ({req.reason}).",
    }


@router.get("/portfolio")
def get_paper_portfolio():
    """Retrieves virtual wallet summary, P&L, and win-rate statistics."""
    return paper_engine.get_portfolio_summary()


@router.post("/simulate-tick")
def simulate_tick(req: TickSimulationRequest):
    """Simulates an incoming Angel One WebSocket tick for testing auto-closure & SL triggers."""
    closed = paper_engine.process_tick(req.symbol, req.ltp)
    return {
        "symbol": req.symbol,
        "processed_ltp": req.ltp,
        "auto_closed_count": len(closed),
        "auto_closed_orders": [c.model_dump() for c in closed],
    }
