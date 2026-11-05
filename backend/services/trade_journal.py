"""Automated Trade Journal Service for TRADEx-ALGO.

Records, stores, and analyzes every Paper, Live, and Manual trade with full
friction accounting, strategy tagging, and institutional summary performance metrics.
Persisted locally in backend/data/trade_journal.json.
"""

import os
import json
import time
import uuid
import threading
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from services.friction_engine import calculate_trade_friction

logger = logging.getLogger("trade_journal")

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(CURRENT_DIR)
JOURNAL_FILE_PATH = os.path.join(BACKEND_DIR, "data", "trade_journal.json")


class TradeJournalManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.trades: List[Dict[str, Any]] = []
        self._load()

    def _load(self):
        if os.path.exists(JOURNAL_FILE_PATH):
            try:
                with open(JOURNAL_FILE_PATH, "r", encoding="utf-8") as f:
                    self.trades = json.load(f)
                logger.info(f"Loaded {len(self.trades)} journaled trades from disk.")
            except Exception as e:
                logger.warning(f"Journal load error: {e}")
                self.trades = []
        else:
            self._seed_default_journal()

    def _seed_default_journal(self):
        """Pre-seeds realistic sample trades across Paper, Live, and Manual modes if file doesn't exist."""
        samples = [
            {
                "id": "tj_" + str(uuid.uuid4())[:8],
                "timestamp": "2026-11-04T10:15:00",
                "symbol": "NIFTY",
                "instrument": "NIFTY 23950 CE",
                "strategy": "ORB Breakout",
                "mode": "PAPER",
                "direction": "LONG",
                "entry_price": 142.50,
                "exit_price": 188.00,
                "qty": 50,
                "gross_pnl": 2275.0,
                "total_friction": 86.42,
                "net_pnl": 2188.58,
                "status": "CLOSED",
                "exit_reason": "Target Hit (2R)",
                "notes": "15m ORB High breakout at 09:30 candle close",
            },
            {
                "id": "tj_" + str(uuid.uuid4())[:8],
                "timestamp": "2026-11-04T11:45:00",
                "symbol": "BANKNIFTY",
                "instrument": "BANKNIFTY 55900 PE",
                "strategy": "VWAP Alpha",
                "mode": "LIVE",
                "direction": "SHORT",
                "entry_price": 280.00,
                "exit_price": 245.00,
                "qty": 30,
                "gross_pnl": 1050.0,
                "total_friction": 78.15,
                "net_pnl": 971.85,
                "status": "CLOSED",
                "exit_reason": "Trailing Stop",
                "notes": "Rejected VWAP from below; premium decayed nicely",
            },
            {
                "id": "tj_" + str(uuid.uuid4())[:8],
                "timestamp": "2026-11-04T13:20:00",
                "symbol": "RELIANCE",
                "instrument": "RELIANCE EQ",
                "strategy": "Manual Desk",
                "mode": "MANUAL",
                "direction": "LONG",
                "entry_price": 1308.50,
                "exit_price": 1315.00,
                "qty": 100,
                "gross_pnl": 650.0,
                "total_friction": 52.30,
                "net_pnl": 597.70,
                "status": "CLOSED",
                "exit_reason": "Discretionary Exit",
                "notes": "Intraday heavyweights momentum scalp",
            },
        ]
        self.trades = samples
        self._save()

    def _save(self):
        try:
            os.makedirs(os.path.dirname(JOURNAL_FILE_PATH), exist_ok=True)
            with open(JOURNAL_FILE_PATH, "w", encoding="utf-8") as f:
                json.dump(self.trades, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving trade journal: {e}")

    def add_trade(
        self,
        symbol: str,
        instrument: str,
        strategy: str,
        mode: str,
        direction: str,
        entry_price: float,
        exit_price: float,
        qty: int,
        exit_reason: str = "Exit",
        notes: str = "",
        custom_friction: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Records a new completed trade into the journal with exact friction accounting."""
        with self.lock:
            inst_type = "OPTION" if ("CE" in instrument.upper() or "PE" in instrument.upper()) else (
                "FUTURES" if "FUT" in instrument.upper() else "EQUITY"
            )
            friction_res = custom_friction or calculate_trade_friction(
                instrument_type=inst_type,
                entry_price=entry_price,
                exit_price=exit_price,
                qty=qty,
                is_buy_first=(direction.upper() == "LONG"),
            )

            record = {
                "id": "tj_" + str(uuid.uuid4())[:8],
                "timestamp": datetime.now().isoformat(),
                "symbol": symbol.upper().strip(),
                "instrument": instrument.upper().strip(),
                "strategy": strategy.strip(),
                "mode": mode.upper().strip(),  # PAPER, LIVE, MANUAL
                "direction": direction.upper().strip(),
                "entry_price": round(entry_price, 2),
                "exit_price": round(exit_price, 2),
                "qty": qty,
                "gross_pnl": friction_res["gross_pnl"],
                "total_friction": friction_res["total_friction"],
                "net_pnl": friction_res["net_pnl"],
                "friction_breakdown": friction_res.get("breakdown", {}),
                "status": "CLOSED",
                "exit_reason": exit_reason,
                "notes": notes,
            }

            self.trades.insert(0, record)  # Newest first
            if len(self.trades) > 1000:
                self.trades = self.trades[:1000]
            self._save()
            return record

    def get_summary_and_trades(
        self,
        strategy_filter: Optional[str] = None,
        mode_filter: Optional[str] = None,
        symbol_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Computes aggregate analytics and filters journal entries."""
        with self.lock:
            filtered = list(self.trades)

            if strategy_filter:
                filtered = [t for t in filtered if strategy_filter.lower() in t.get("strategy", "").lower()]
            if mode_filter:
                filtered = [t for t in filtered if t.get("mode", "").upper() == mode_filter.upper()]
            if symbol_filter:
                filtered = [t for t in filtered if t.get("symbol", "").upper() == symbol_filter.upper()]

            total_trades = len(filtered)
            if total_trades == 0:
                return {
                    "total_trades": 0,
                    "winning_trades": 0,
                    "losing_trades": 0,
                    "win_rate": 0.0,
                    "gross_pnl": 0.0,
                    "total_friction": 0.0,
                    "net_pnl": 0.0,
                    "profit_factor": 0.0,
                    "expectancy": 0.0,
                    "trades": [],
                }

            wins = [t for t in filtered if t.get("net_pnl", 0) > 0]
            losses = [t for t in filtered if t.get("net_pnl", 0) <= 0]
            win_rate = round((len(wins) / total_trades) * 100, 2)

            gross_pnl = round(sum(t.get("gross_pnl", 0) for t in filtered), 2)
            total_friction = round(sum(t.get("total_friction", 0) for t in filtered), 2)
            net_pnl = round(sum(t.get("net_pnl", 0) for t in filtered), 2)

            gross_wins = sum(t.get("gross_pnl", 0) for t in wins)
            gross_losses = abs(sum(t.get("gross_pnl", 0) for t in losses))
            profit_factor = round(gross_wins / max(gross_losses, 1.0), 2)
            expectancy = round(net_pnl / total_trades, 2)

            # Strategy breakdown
            by_strat: Dict[str, Dict[str, Any]] = {}
            for t in filtered:
                st = t.get("strategy", "Unknown")
                if st not in by_strat:
                    by_strat[st] = {"count": 0, "net_pnl": 0.0, "wins": 0}
                by_strat[st]["count"] += 1
                by_strat[st]["net_pnl"] += t.get("net_pnl", 0)
                if t.get("net_pnl", 0) > 0:
                    by_strat[st]["wins"] += 1

            best_strat = "None"
            best_strat_pnl = -9999999.0
            for st, d in by_strat.items():
                if d["net_pnl"] > best_strat_pnl:
                    best_strat_pnl = d["net_pnl"]
                    best_strat = st

            return {
                "total_trades": total_trades,
                "winning_trades": len(wins),
                "losing_trades": len(losses),
                "win_rate": win_rate,
                "gross_pnl": gross_pnl,
                "total_friction": total_friction,
                "net_pnl": net_pnl,
                "profit_factor": profit_factor,
                "expectancy": expectancy,
                "best_strategy": best_strat,
                "strategy_breakdown": by_strat,
                "trades": filtered,
            }

    def clear(self):
        with self.lock:
            self.trades = []
            self._save()


trade_journal = TradeJournalManager()
