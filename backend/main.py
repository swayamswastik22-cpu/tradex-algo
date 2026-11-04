"""Indian Algo Trader - Complete FastAPI Backend.

Main application entrypoint integrating:
1. Headless NSE Option Chain Service (/api/option-chain)
2. Indian Stock Market & Day Trader ORB Engine (/api/market/*, /api/trader/*)
3. 100% Virtual Paper Trading OMS (/api/paper/*)
4. Optopsy Options Strategy Backtester (/api/optopsy-simulate)
5. Angel One SmartWebSocketV2 Background Ticks Thread
6. Production Health Check & CORS Middleware (allowing *)
"""

import os
import sys
import time
import json
import requests
import random
import logging
import threading
import asyncio
import gc
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse

# Ensure backend folder is on python path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

# Auto-load .env file
from dotenv import load_dotenv
ENV_PATH = os.path.join(CURRENT_DIR, ".env")
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

from services.option_chain_service import router as option_chain_router
from services.day_trader_nse import router as day_trader_router
from services.paper_oms import router as paper_oms_router, paper_engine
from services.backtester import router as backtester_router
from services.copy_trading_engine import router as copy_trading_router
from services.candle_engine import candle_engine
from services.angel_auth import angel_auth
from routers.scanner import router as scanner_router
from routers.option_chain import router as dynamic_option_chain_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("indian_algo_trader")

APP_START_TIME = time.time()


# ==========================================================
# Load 218 FNO Stocks Dataset & Initial Prices
# ==========================================================
from collections import defaultdict

DATA_FILE_CANDIDATES = [
    os.path.join(CURRENT_DIR, "data", "fno_stocks.json"),
    os.path.join(os.path.dirname(CURRENT_DIR), "data", "fno_stocks.json"),
    os.path.join(CURRENT_DIR, "data", "nse_stocks.json"),
]
ALL_STOCK_SYMBOLS: List[str] = []
PREVIOUS_CLOSES: Dict[str, float] = {}
INITIAL_LTP_PRICES: Dict[str, float] = {}

for data_path in DATA_FILE_CANDIDATES:
    if os.path.exists(data_path):
        try:
            with open(data_path, "r", encoding="utf-8") as f:
                loaded_stocks = json.load(f)
                for s in loaded_stocks:
                    sym = s.get("symbol", "").upper().strip()
                    if sym and sym not in ALL_STOCK_SYMBOLS:
                        ALL_STOCK_SYMBOLS.append(sym)
            logger.info(f"Loaded {len(ALL_STOCK_SYMBOLS)} FNO stock symbols from {data_path} (live quotes will be fetched via Angel One).")
            break
        except Exception as e:
            logger.warning(f"Failed to load {data_path}: {e}")

# ==========================================================
# Angel One SmartWebSocketV2 Background Manager Thread
# ==========================================================
TOKEN_TO_SYMBOL: Dict[str, str] = {
    "2885": "RELIANCE",
    "1594": "INFY",
    "11536": "TCS",
    "3045": "SBIN",
    "1333": "HDFCBANK",
    "4963": "ICICIBANK",
    "1660": "ITC",
    "11483": "LT",
    "3456": "TATAMOTORS",
    "26000": "NIFTY",
    "99926000": "NIFTY",
    "26009": "BANKNIFTY",
    "99926009": "BANKNIFTY",
}

# Active WebSocket connections to /ws/stream & global event loop for immediate threadsafe broadcast
ACTIVE_WS_CLIENTS: List[WebSocket] = []
GLOBAL_EVENT_LOOP: Optional[asyncio.AbstractEventLoop] = None


def broadcast_tick_immediate(sym: str, token: str, price_val: float, high: float, low: float, volume: int, change: float, pct: float, prev_close: float = 0.0):
    """Broadcasts a live market tick immediately to all connected /ws/stream clients."""
    if not ACTIVE_WS_CLIENTS or not GLOBAL_EVENT_LOOP or not GLOBAL_EVENT_LOOP.is_running():
        return
    try:
        payload = json.dumps({
            "type": "tick",
            "symbol": sym,
            "token": token,
            "ltp": price_val,
            "current_price": price_val,
            "prev_close": prev_close,
            "previous_close": prev_close,
            "close": prev_close,
            "high": high,
            "low": low,
            "volume": volume,
            "change": change,
            "net_change": change,
            "pct_change": pct,
            "change_pct": pct,
            "percent_change": pct,
            "percentChange": pct,
            "timestamp": time.time(),
        })

        async def _send_all():
            dead = []
            for ws in list(ACTIVE_WS_CLIENTS):
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.append(ws)
            for d in dead:
                if d in ACTIVE_WS_CLIENTS:
                    ACTIVE_WS_CLIENTS.remove(d)

        asyncio.run_coroutine_threadsafe(_send_all(), GLOBAL_EVENT_LOOP)
    except Exception:
        pass


class AngelOneWebSocketManager:
    """Manages live market tick consumption from Angel One SmartWebSocketV2

    with automatic TOTP session initialization, OpenAPIScripMaster token extraction,
    and streaming to Virtual Paper OMS and global state.
    """

    def __init__(self):
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.quote_thread: Optional[threading.Thread] = None
        self.sws: Optional[Any] = None
        self._scrip_lock = threading.Lock()
        self._scrip_loaded = threading.Event()
        angel_auth.reload_env()
        self.api_key = angel_auth.api_key
        self.client_code = angel_auth.client_code
        self.pin = angel_auth.pin
        self.totp_secret = angel_auth.totp_secret
        self.user_name = ""
        self.ticks_count = 0
        self.last_tick_time = ""
        self.latest_prices: Dict[str, float] = {}
        self.market_state: Dict[str, Dict[str, Any]] = {}
        self.nfo_contracts_by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.previous_closes: Dict[str, float] = {}
        self.token_to_symbol: Dict[str, str] = dict(TOKEN_TO_SYMBOL)
        self.symbol_to_token: Dict[str, int] = {}
        self.resolved_integer_tokens: Dict[str, List[int]] = {"NSE": [], "INDICES": []}
        self.unmatched_symbols: List[str] = []

        # Pre-seed initial integer token mappings so /api/state and /api/trader/scanner respond in <50ms without blocking startup
        default_map = {
            2885: "RELIANCE", 1333: "HDFCBANK", 11536: "TCS", 1594: "INFY", 3045: "SBIN",
            4963: "ICICIBANK", 11483: "LT", 1660: "ITC", 317: "BAJFINANCE", 1232: "HCLTECH",
            10604: "BHARTIARTL", 1922: "KOTAKBANK", 3787: "WIPRO", 3456: "TATAMOTORS",
            26000: "NIFTY", 26009: "BANKNIFTY", 99926000: "NIFTY", 99926009: "BANKNIFTY",
            99919000: "SENSEX", 99926017: "INDIA VIX",
        }
        for tok, sym in default_map.items():
            self.token_to_symbol[str(tok)] = sym
            self.symbol_to_token[sym] = tok
        self.resolved_integer_tokens = {
            "NSE": [tok for tok in default_map if tok not in [26000, 26009, 99926000, 99926009, 99919000, 99926017]],
            "INDICES": [99926000, 99926009, 99919000, 99926017]
        }

        self.mode = (
            "LIVE (Angel One SmartWebSocketV2)"
            if angel_auth.is_configured()
            else "STANDBY"
        )

    def start(self):
        self.running = True
        # Run ScripMaster download and token indexing in a background daemon thread so Uvicorn port binds instantly
        threading.Thread(target=self._fetch_scrip_master, daemon=True, name="ScripMasterLoader").start()
        self.thread = threading.Thread(target=self._run_loop, daemon=True, name="AngelOneFeedThread")
        self.thread.start()
        self.quote_thread = threading.Thread(target=self._run_market_data_poll_loop, daemon=True, name="AngelOneQuotePoller")
        self.quote_thread.start()
        logger.info(f"Angel One Market Feed & REST Quote Poller started in {self.mode} mode.")

    def stop(self):
        self.running = False

    def _fetch_scrip_master(self) -> Tuple[List[int], List[int], Dict[str, str], Dict[str, int]]:
        """Downloads Angel One OpenAPIScripMaster.json in a background thread, strictly filters for the

        218 FNO symbols + 4 indices, frees the raw list with gc.collect() to keep RAM < 150MB, and indexes tokens.
        """
        with self._scrip_lock:
            if self._scrip_loaded.is_set() and len(self.resolved_integer_tokens.get("NSE", [])) > 20:
                return (
                    self.resolved_integer_tokens["NSE"],
                    self.resolved_integer_tokens.get("INDICES", []),
                    self.token_to_symbol,
                    self.symbol_to_token,
                )

            target_equities = set(ALL_STOCK_SYMBOLS) if ALL_STOCK_SYMBOLS else {
                "RELIANCE", "HDFCBANK", "TCS", "INFY", "SBIN", "ICICIBANK", "LT", "ITC"
            }
            target_names = {s.upper().strip() for s in target_equities}
            target_names.update([
                "NIFTY", "BANKNIFTY", "SENSEX", "INDIA VIX", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"
            ])
            
            nse_equity_tokens: List[int] = []
            index_tokens: List[int] = []
            token_to_symbol: Dict[str, str] = dict(self.token_to_symbol)
            symbol_to_token: Dict[str, int] = dict(self.symbol_to_token)
            self.unmatched_symbols = []

            local_cache = os.path.join(CURRENT_DIR, "OpenAPIScripMaster.json")
            raw_instruments = None

            # 1. Download live Angel One Scrip Master if not already loaded or cache older than 12h
            is_fresh_cache = os.path.exists(local_cache) and (time.time() - os.path.getmtime(local_cache) < 43200)
            if is_fresh_cache:
                try:
                    with open(local_cache, "r", encoding="utf-8") as f:
                        raw_instruments = json.load(f)
                    logger.info(f"Loaded {len(raw_instruments)} instruments from fresh daily ScripMaster cache.")
                except Exception as e:
                    logger.warning(f"Failed to read local cache: {e}")

            if not raw_instruments:
                urls = [
                    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
                    "https://margincalculator.angelbroking.com/OpenAPI_File/OpenAPIScripMaster.json",
                    "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json",
                ]
                for url in urls:
                    try:
                        logger.info(f"Downloading live Angel One ScripMaster from: {url}")
                        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
                        if resp.status_code == 200 and "application/json" in resp.headers.get("content-type", ""):
                            raw_instruments = resp.json()
                            try:
                                with open(local_cache, "w", encoding="utf-8") as f:
                                    json.dump(raw_instruments, f)
                                logger.info(f"Downloaded and cached {len(raw_instruments)} instruments from {url}.")
                            except Exception:
                                pass
                            break
                    except Exception as ex:
                        logger.warning(f"Live ScripMaster download attempt failed ({url}): {ex}")

            # Fallback to local cache if network download was unreachable
            if not raw_instruments and os.path.exists(local_cache):
                try:
                    with open(local_cache, "r", encoding="utf-8") as f:
                        raw_instruments = json.load(f)
                    logger.info(f"Loaded {len(raw_instruments)} instruments from local ScripMaster cache.")
                except Exception as e:
                    logger.warning(f"Failed to read local cache: {e}")

            if raw_instruments:
                # Keep ONLY rows where name or symbol is in the 218 FNO symbols + 4 indices
                filtered_instruments = []
                for item in raw_instruments:
                    clean_name = str(item.get("name", "")).upper().strip().replace("-EQ", "")
                    sym = str(item.get("symbol", "")).upper().strip()
                    tok = str(item.get("token", "")).strip()
                    if (
                        clean_name in target_names
                        or sym.replace("-EQ", "") in target_names
                        or tok in ["99926000", "26000", "99926009", "26009", "99919000", "99926017"]
                    ):
                        filtered_instruments.append(item)

                # IMMEDIATELY delete raw list and invoke garbage collection so RAM stays <150MB
                del raw_instruments
                gc.collect()
                logger.info(f"Filtered ScripMaster to {len(filtered_instruments)} relevant instruments (RAM protected).")

                # Pre-index scrip master for 4-tier lookup
                chain_a: Dict[Tuple[str, str], Dict[str, Any]] = {}
                chain_b: Dict[Tuple[str, str], Dict[str, Any]] = {}
                chain_c: Dict[Tuple[str, str], Dict[str, Any]] = {}
                chain_d: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)

                for item in filtered_instruments:
                    sym = str(item.get("symbol", "")).upper().strip()
                    name = str(item.get("name", "")).upper().strip()
                    exch = str(item.get("exch_seg", "")).upper().strip()
                    itype = str(item.get("instrumenttype", "")).upper().strip()
                    token_raw = item.get("token")
                    if not token_raw:
                        continue
                    try:
                        token_int = int(token_raw)
                    except (ValueError, TypeError):
                        continue

                    # Strict Match Indices (tokens 99926000, 99926009, 99919000, 99926017 and 26000, 26009)
                    if token_int in [99926000, 26000]:
                        if token_int not in index_tokens:
                            index_tokens.append(token_int)
                        token_to_symbol[str(token_int)] = "NIFTY"
                        symbol_to_token["NIFTY"] = token_int
                    elif token_int in [99926009, 26009]:
                        if token_int not in index_tokens:
                            index_tokens.append(token_int)
                        token_to_symbol[str(token_int)] = "BANKNIFTY"
                        symbol_to_token["BANKNIFTY"] = token_int
                    elif token_int == 99919000:
                        if token_int not in index_tokens:
                            index_tokens.append(token_int)
                        token_to_symbol[str(token_int)] = "SENSEX"
                        symbol_to_token["SENSEX"] = token_int
                    elif token_int == 99926017:
                        if token_int not in index_tokens:
                            index_tokens.append(token_int)
                        token_to_symbol[str(token_int)] = "INDIA VIX"
                        symbol_to_token["INDIA VIX"] = token_int

                    # Build Chains for Cash & Derivatives
                    if exch == "NSE":
                        chain_a[(exch, sym)] = item
                        if sym.endswith("-EQ"):
                            chain_b[(exch, name)] = item
                        if itype in ["", "EQ", "BE"]:
                            chain_c[(exch, name)] = item
                    elif exch == "NFO":
                        if itype == "FUTSTK":
                            chain_d[(exch, name)].append(item)
                        clean_name = name.replace("-EQ", "").strip().upper()
                        if clean_name in target_equities or clean_name in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
                            self.nfo_contracts_by_symbol[clean_name].append(item)

                # Resolve every symbol using the strict 4-tier fallback chain
                for target_sym in sorted(target_equities):
                    item = None
                    if ("NSE", f"{target_sym}-EQ") in chain_a:
                        item = chain_a[("NSE", f"{target_sym}-EQ")]
                    elif ("NSE", target_sym) in chain_b:
                        item = chain_b[("NSE", target_sym)]
                    elif ("NSE", target_sym) in chain_c:
                        item = chain_c[("NSE", target_sym)]
                    elif ("NFO", target_sym) in chain_d and chain_d[("NFO", target_sym)]:
                        item = chain_d[("NFO", target_sym)][0]

                    if item:
                        tok_raw = item.get("token")
                        try:
                            tok_int = int(tok_raw)
                            if tok_int not in nse_equity_tokens:
                                nse_equity_tokens.append(tok_int)
                            token_to_symbol[str(tok_int)] = target_sym
                            symbol_to_token[target_sym] = tok_int
                        except (ValueError, TypeError):
                            self.unmatched_symbols.append(target_sym)
                    else:
                        self.unmatched_symbols.append(target_sym)

                del filtered_instruments
                gc.collect()

            # Guaranteed fallbacks for exact integer tokens
            default_map = {
                2885: "RELIANCE", 1333: "HDFCBANK", 11536: "TCS", 1594: "INFY", 3045: "SBIN",
                4963: "ICICIBANK", 11483: "LT", 1660: "ITC", 317: "BAJFINANCE", 1232: "HCLTECH",
                10604: "BHARTIARTL", 1922: "KOTAKBANK", 3787: "WIPRO", 3456: "TATAMOTORS",
                26000: "NIFTY", 26009: "BANKNIFTY", 99926000: "NIFTY", 99926009: "BANKNIFTY",
                99919000: "SENSEX", 99926017: "INDIA VIX",
            }
            for tok, sym in default_map.items():
                token_to_symbol[str(tok)] = sym
                if sym not in symbol_to_token:
                    symbol_to_token[sym] = tok
                if tok in [26000, 26009, 99926000, 99926009, 99919000, 99926017]:
                    if tok not in index_tokens:
                        index_tokens.append(tok)
                else:
                    if tok not in nse_equity_tokens:
                        nse_equity_tokens.append(tok)

            self.token_to_symbol = token_to_symbol
            self.symbol_to_token = symbol_to_token
            self.resolved_integer_tokens = {"NSE": nse_equity_tokens, "INDICES": index_tokens}
            self._scrip_loaded.set()
            logger.info(
                f"Resolved Token Lookup: {len(symbol_to_token)} symbols, {len(nse_equity_tokens)} Equity tokens, {len(index_tokens)} Index tokens."
            )
            return nse_equity_tokens, index_tokens, token_to_symbol, symbol_to_token

    def _run_loop(self):
        if "LIVE" in self.mode:
            self._connect_live_smart_websocket()
        else:
            self._run_simulated_tick_feed()

    def _connect_live_smart_websocket(self):
        """Authenticates via Angel One TOTP session and streams live binary ticks via SmartWebSocketV2."""
        while self.running:
            try:
                from SmartApi.smartWebSocketV2 import SmartWebSocketV2

                # 1. Dynamic authentication using fresh TOTP
                auth_res = angel_auth.generate_session()
                if not auth_res.get("status"):
                    logger.warning(f"Angel One session failed: {auth_res.get('message')}. Retrying in 15s...")
                    time.sleep(15)
                    continue

                jwt_token = auth_res["jwtToken"]
                feed_token = auth_res["feedToken"]
                client_code = auth_res["client_code"]
                api_key = auth_res["api_key"]
                self.user_name = auth_res["name"]

                # 2. Build fast dictionary lookup and resolved integer tokens
                equity_tokens, idx_tokens, token_map, sym_map = self._fetch_scrip_master()
                self.token_to_symbol = token_map
                self.symbol_to_token = sym_map
                self.resolved_integer_tokens = {"NSE": equity_tokens, "INDICES": idx_tokens}
                logger.info(
                    f"Resolved Token Lookup: {len(sym_map)} symbols, {len(equity_tokens)} Equity tokens, {len(idx_tokens)} Index tokens."
                )

                # Pre-fetch official close prices via getMarketData so ticks have accurate today's % change immediately
                try:
                    self.fetch_live_market_quotes_batch()
                except Exception as ex:
                    logger.warning(f"Pre-WS market quote batch notice: {ex}")

                # 3. Initialize SmartWebSocketV2 directly with fresh tokens
                sws = SmartWebSocketV2(
                    auth_token=jwt_token,
                    api_key=api_key,
                    client_code=client_code,
                    feed_token=feed_token,
                    retry_strategy=1,
                    retry_delay=5,
                    max_retry_attempt=10,
                )
                self.sws = sws

                def on_data(*args, **kwargs):
                    try:
                        # SmartWebSocketV2 passes (wsapp, message)
                        if len(args) >= 2:
                            message = args[1]
                        elif len(args) == 1:
                            message = args[0]
                        else:
                            message = kwargs.get("message") or kwargs.get("data", {})

                        if not isinstance(message, dict):
                            return

                        token = str(message.get("token", "")).strip()
                        raw_ltp = message.get("last_traded_price") or message.get("ltp") or 0.0
                        ltp = float(raw_ltp) / 100.0 if raw_ltp > 0 else 0.0

                        raw_high = message.get("high_price_of_the_day") or message.get("high") or message.get("high_price") or 0.0
                        day_high = float(raw_high) / 100.0 if raw_high > 0 else None

                        raw_low = message.get("low_price_of_the_day") or message.get("low") or message.get("low_price") or 0.0
                        day_low = float(raw_low) / 100.0 if raw_low > 0 else None

                        raw_close = message.get("close_price") or message.get("prev_close_price") or message.get("close") or 0.0
                        close_price_val = float(raw_close) / 100.0 if raw_close > 0 else None

                        volume = int(message.get("volume_traded_for_the_day") or message.get("volume") or 0)
                        sym = self.token_to_symbol.get(token) or TOKEN_TO_SYMBOL.get(token, f"NSE_{token}")

                        if ltp > 0:
                            price_val = round(ltp, 2)
                            now_dt = datetime.now()
                            now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
                            now_iso = now_dt.isoformat()
                            now_ts = now_dt.timestamp()

                            # Print received tick
                            print(f"[TICK] {sym:<10} (Token: {token:<8}) -> LTP: Rs {price_val:>9.2f} | Time: {now_str}", flush=True)

                            self.ticks_count += 1
                            self.last_tick_time = now_iso
                            self.latest_prices[sym] = price_val
                            self.latest_prices[token] = price_val

                            if close_price_val and close_price_val > 0:
                                self.previous_closes[sym] = close_price_val
                                self.previous_closes[token] = close_price_val
                                PREVIOUS_CLOSES[sym] = close_price_val

                            st = self.market_state.get(sym) or {}
                            prev_close = (
                                self.previous_closes.get(sym)
                                or self.previous_closes.get(token)
                                or PREVIOUS_CLOSES.get(sym)
                                or PREVIOUS_CLOSES.get(token)
                                or st.get("prev_close")
                                or st.get("close")
                                or st.get("previous_close")
                                or price_val
                            )
                            final_high = day_high if day_high else max(price_val, prev_close)
                            final_low = day_low if day_low else min(price_val, prev_close)
                            chg = round(price_val - prev_close, 2)
                            pct = round(((price_val - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0.0

                            final_high = max(st.get("high", final_high), final_high)
                            final_low = min(st.get("low", final_low), final_low)

                            orb_high = st.get("orb_high", round(prev_close * 1.006, 2))
                            orb_low = st.get("orb_low", round(prev_close * 0.994, 2))
                            if now_dt.hour == 9 and 15 <= now_dt.minute <= 30:
                                orb_high = max(orb_high, price_val)
                                orb_low = min(orb_low, price_val)

                            tick_record = {
                                "token": token,
                                "symbol": sym,
                                "ltp": price_val,
                                "current_price": price_val,
                                "high": round(final_high, 2),
                                "low": round(final_low, 2),
                                "open": price_val,
                                "prev_close": prev_close,
                                "previous_close": prev_close,
                                "close": prev_close,
                                "change": chg,
                                "net_change": chg,
                                "netChange": chg,
                                "pct_change": pct,
                                "change_pct": pct,
                                "percent_change": pct,
                                "percentChange": pct,
                                "volume": volume,
                                "orb_high": round(orb_high, 2),
                                "orb_low": round(orb_low, 2),
                                "last_tick_time": now_iso,
                                "feed_source": "Angel One SmartWebSocketV2 (Live)",
                            }
                            # Update global market_state for both symbol and token
                            self.market_state[sym] = tick_record
                            self.market_state[token] = tick_record

                            candle_engine.add_tick(sym, price_val, now_ts)
                            paper_engine.process_tick(sym, price_val)

                            # Broadcast immediately to all connected frontend clients
                            broadcast_tick_immediate(sym, token, price_val, final_high, final_low, volume, chg, pct, prev_close)

                    except Exception as ex:
                        logger.error(f"Tick parse error: {ex}")

                def on_open(wsapp):
                    logger.info("Angel One SmartWebSocketV2 connected! Subscribing in 5 batches of 45...")
                    print(
                        f"[WS OPEN] Subscribing {len(equity_tokens)} equities & {len(idx_tokens)} indices in 45-token chunks (Quote mode)...",
                        flush=True,
                    )

                    def chunks(lst, n):
                        for i in range(0, len(lst), n):
                            yield lst[i:i + n]

                    mode = 2  # Quote mode (mode 1: LTP, mode 2: Quote, mode 3: Snap quote)
                    # Chunk subscriptions to prevent socket reset by Angel One server
                    for idx, chunk in enumerate(chunks(equity_tokens, 45)):
                        token_payload = [{"exchangeType": 1, "tokens": [str(t) for t in chunk]}]
                        sws.subscribe(f"tradex_stream_eq_{idx}", mode, token_payload)
                        logger.info(f"Subscribed equity chunk {idx + 1} ({len(chunk)} tokens) in Quote mode")
                        time.sleep(0.5)

                    if idx_tokens:
                        sws.subscribe(
                            "tradex_stream_idx_3",
                            mode,
                            [{"exchangeType": 3, "tokens": [str(t) for t in idx_tokens]}]
                        )
                        time.sleep(0.5)
                        sws.subscribe(
                            "tradex_stream_idx_1",
                            mode,
                            [{"exchangeType": 1, "tokens": [str(t) for t in idx_tokens]}]
                        )
                    print("[WS OPEN] All token batches successfully subscribed in Quote mode.", flush=True)

                def on_error(*args, **kwargs):
                    logger.warning(f"Angel One WebSocket Error: {args} {kwargs}")

                def on_close(*args, **kwargs):
                    logger.info(f"Angel One WebSocket connection closed: {args} {kwargs}")

                sws.on_data = on_data
                sws.on_open = on_open
                sws.on_error = on_error
                sws.on_close = on_close

                sws.connect()
            except Exception as e:
                logger.warning(f"Angel One WebSocket loop notice: {e}. Reconnecting in 5s...")
                time.sleep(5)

    def fetch_live_market_quotes_batch(self):
        """Fetches real-time market quotes via Angel One REST API getMarketData for all 218 FNO stocks in chunks of 45."""
        if not angel_auth.smart_api or not angel_auth.jwt_token:
            angel_auth.generate_session()
        if not angel_auth.smart_api:
            return

        tokens = self.resolved_integer_tokens.get("NSE", [])
        if not tokens:
            eq_toks, idx_toks, tok_map, sym_map = self._fetch_scrip_master()
            self.token_to_symbol.update(tok_map)
            self.symbol_to_token.update(sym_map)
            tokens = eq_toks
            self.resolved_integer_tokens = {"NSE": eq_toks, "INDICES": idx_toks}

        if not tokens:
            return

        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        for chunk in chunks(tokens, 45):
            try:
                token_strs = [str(t) for t in chunk]
                resp = angel_auth.smart_api.getMarketData("FULL", {"NSE": token_strs})
                if resp and not resp.get("status"):
                    if angel_auth.handle_api_response(resp):
                        resp = angel_auth.smart_api.getMarketData("FULL", {"NSE": token_strs})
                    else:
                        break
                if resp and resp.get("status") and resp.get("data", {}).get("fetched"):
                    fetched_list = resp["data"]["fetched"]
                    now_iso = datetime.now().isoformat()
                    now_ts = time.time()
                    for item in fetched_list:
                        tok = str(item.get("symbolToken", "")).strip()
                        trading_sym = str(item.get("tradingSymbol", "")).strip()
                        clean_sym = trading_sym.replace("-EQ", "").strip().upper()
                        sym = self.token_to_symbol.get(tok) or clean_sym

                        ltp = float(item.get("ltp") or 0.0)
                        if ltp <= 0:
                            continue

                        close_p = float(item.get("close") or 0.0)
                        if close_p <= 0:
                            close_p = ltp

                        net_change = float(item.get("netChange") if item.get("netChange") is not None else (ltp - close_p))
                        chg = round(net_change, 2)
                        pct_change = round(((ltp - close_p) / close_p) * 100, 2) if close_p > 0 else float(item.get("percentChange", 0.0) or 0.0)
                        high_p = float(item.get("high") or ltp)
                        low_p = float(item.get("low") or ltp)
                        open_p = float(item.get("open") or ltp)
                        vol = int(item.get("tradeVolume") or 0)
                        oi = int(item.get("opnInterest") or 0)

                        # Update in-memory state with 100% genuine live prices
                        self.latest_prices[sym] = ltp
                        self.latest_prices[tok] = ltp
                        self.previous_closes[sym] = close_p
                        self.previous_closes[tok] = close_p
                        PREVIOUS_CLOSES[sym] = close_p

                        or_high = round(close_p * 1.006, 2)
                        or_low = round(close_p * 0.994, 2)

                        record = {
                            "token": tok,
                            "symbol": sym,
                            "ltp": ltp,
                            "current_price": ltp,
                            "open": open_p,
                            "high": high_p,
                            "low": low_p,
                            "close": close_p,
                            "prev_close": close_p,
                            "previous_close": close_p,
                            "change": chg,
                            "net_change": chg,
                            "netChange": chg,
                            "pct_change": pct_change,
                            "change_pct": pct_change,
                            "percent_change": pct_change,
                            "percentChange": pct_change,
                            "volume": vol,
                            "oi": oi,
                            "orb_high": or_high,
                            "orb_low": or_low,
                            "last_tick_time": now_iso,
                            "feed_source": "Angel One REST MarketData (Live)",
                        }
                        self.market_state[sym] = record
                        self.market_state[tok] = record
                        self.last_tick_time = now_iso
                        self.ticks_count += 1

                        candle_engine.add_tick(sym, ltp, now_ts)
                        paper_engine.process_tick(sym, ltp)

            except Exception as ex:
                logger.debug(f"Market quotes batch fetch note: {ex}")
            time.sleep(1.2)

    def _run_market_data_poll_loop(self):
        """Continuously polls Angel One getMarketData every 3 seconds to keep all 218 FNO quotes fresh."""
        logger.info("Angel One REST MarketData Poller started (3s interval).")
        while self.running:
            try:
                self.fetch_live_market_quotes_batch()
                time.sleep(3)
            except Exception as e:
                logger.warning(f"MarketData poller loop note: {e}")
                time.sleep(3)

    def _run_simulated_tick_feed(self):
        """Fallback poller that delegates to live batch quote fetcher."""
        while self.running:
            try:
                self.fetch_live_market_quotes_batch()
                time.sleep(3)
            except Exception:
                time.sleep(3)

    def subscribe_instrument(self, symbol: str, token: str, exch_seg: str = "NSE") -> bool:
        """Dynamically subscribes a newly selected symbol/token on the active SmartWebSocketV2 feed."""
        exch_type = 1
        exch_upper = str(exch_seg).upper()
        if exch_upper in ["NFO", "BFO", "FO"]:
            exch_type = 2
        elif exch_upper in ["BSE", "BSE_CM"]:
            exch_type = 3
        elif exch_upper in ["MCX", "MCX_FO"]:
            exch_type = 5
        elif symbol in ["NIFTY", "BANKNIFTY"]:
            exch_type = 3

        token_str = str(token).strip()
        self.token_to_symbol[token_str] = symbol
        try:
            self.symbol_to_token[symbol] = int(token_str)
        except ValueError:
            self.symbol_to_token[symbol] = token_str

        if self.sws:
            try:
                corr_id = f"dyn_{token_str[-6:]}"
                token_list = [{"exchangeType": exch_type, "tokens": [token_str]}]
                if exch_type == 3:
                    token_list.append({"exchangeType": 1, "tokens": [token_str]})
                self.sws.subscribe(corr_id, 1, token_list)
                logger.info(f"Dynamically subscribed live WebSocket for {symbol} (Token: {token_str}, Exchange: {exch_type})")
                return True
            except Exception as e:
                logger.warning(f"Failed to dynamically subscribe {symbol}: {e}")
        return False


feed_manager = AngelOneWebSocketManager()


async def broadcast_ws_loop():
    """Broadcasts real-time prices & 20s keep-alive heartbeat to all /ws/stream clients."""
    last_heartbeat_time = time.time()
    while True:
        try:
            await asyncio.sleep(0.5)
            if not ACTIVE_WS_CLIENTS:
                continue

            now_time = time.time()
            # Send real-time tick stream snapshot
            payload = json.dumps({
                "type": "tick_stream",
                "timestamp": datetime.now().isoformat(),
                "live_prices": feed_manager.latest_prices,
                "ticks_count": feed_manager.ticks_count,
                "mode": feed_manager.mode,
                "status": "connected",
            })

            # 20s Render keep-alive heartbeat frame
            send_hb = (now_time - last_heartbeat_time) >= 20.0
            if send_hb:
                hb_payload = json.dumps({
                    "type": "heartbeat",
                    "timestamp": now_time,
                    "status": "alive",
                })
                last_heartbeat_time = now_time

            dead_clients = []
            for client in list(ACTIVE_WS_CLIENTS):
                try:
                    await client.send_text(payload)
                    if send_hb:
                        await client.send_text(hb_payload)
                except Exception:
                    dead_clients.append(client)

            for dc in dead_clients:
                if dc in ACTIVE_WS_CLIENTS:
                    ACTIVE_WS_CLIENTS.remove(dc)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"WS broadcast error: {e}")
            await asyncio.sleep(1)


# ==========================================================
# FastAPI Lifespan & Application Setup
# ==========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global GLOBAL_EVENT_LOOP
    GLOBAL_EVENT_LOOP = asyncio.get_running_loop()
    # Startup: launch background market tick thread & ws broadcaster
    feed_manager.start()
    broadcaster = asyncio.create_task(broadcast_ws_loop())
    yield
    # Shutdown
    broadcaster.cancel()
    feed_manager.stop()


app = FastAPI(
    title="Indian Algo Trader API",
    description=(
        "Production-grade Python 3.12 algorithmic trading backend for Indian NSE/BSE markets. "
        "Integrates Headless NSE Option Chain Analyzer, 15-min ORB Day Trading Strategy Engine, "
        "100% Virtual Paper Trading OMS, and Optopsy Options Backtester."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

# Admin Access PIN for Personal AI Studio Session Lockdown (Owner PIN: 453125)
ADMIN_PIN = str(os.getenv("TRADEX_ADMIN_PIN", "453125")).strip().strip('"').strip("'")

# CORS Middleware: wildcard * with allow_credentials=False for AI Studio preview iframes (*.scf.usercontent.goog)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)


@app.middleware("http")
async def security_and_cors_middleware(request: Request, call_next):
    """Enforces ADMIN_PIN authentication on all /api/* routes and injects CORS headers on every response."""
    # 1. Answer preflight OPTIONS immediately
    if request.method == "OPTIONS":
        response = Response(status_code=200)
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "*"
        response.headers["Access-Control-Expose-Headers"] = "*"
        return response

    path = request.url.path

    # 2. Enforce PIN authentication on all /api/* routes (except /api/auth/verify and root/health)
    if path in ["/", "/health", "/favicon.ico"] or path == "/api/auth/verify":
        pass
    elif path.startswith("/api"):
        auth_pin = (
            request.headers.get("X-Auth-Pin")
            or request.headers.get("x-auth-pin")
            or request.query_params.get("pin")
        )
        if not auth_pin or str(auth_pin).strip() != ADMIN_PIN:
            response = JSONResponse(
                status_code=401,
                content={"detail": "Unauthorized: Invalid or missing X-Auth-Pin header or pin query parameter."},
            )
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Methods"] = "*"
            response.headers["Access-Control-Allow-Headers"] = "*"
            response.headers["Access-Control-Expose-Headers"] = "*"
            return response

    try:
        response = await call_next(request)
    except Exception as exc:
        logger.error(f"Unhandled error handling request: {exc}", exc_info=True)
        response = JSONResponse(
            status_code=500,
            content={"detail": "Internal Server Error", "error": str(exc)},
        )
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Expose-Headers"] = "*"
    return response


@app.api_route("/api/auth/verify", methods=["GET", "POST"], tags=["Auth"])
async def verify_auth_pin(request: Request):
    """Verifies owner PIN (supports both JSON POST and GET ?pin=...) and returns authorization status."""
    pin_val = ""
    if request.method == "GET":
        pin_val = str(request.query_params.get("pin", "")).strip()
    else:
        try:
            body = await request.json()
            pin_val = str(body.get("pin", "")).strip() if isinstance(body, dict) else ""
        except Exception:
            pin_val = str(request.query_params.get("pin", "")).strip()

    if pin_val == ADMIN_PIN:
        return {"authorized": True}
    return JSONResponse(
        status_code=401,
        content={"authorized": False, "detail": "Invalid PIN"},
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler guaranteeing CORS headers even on 500 error responses."""
    logger.error(f"Global exception caught: {exc}", exc_info=True)
    response = JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "error": str(exc)},
    )
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Expose-Headers"] = "*"
    return response

# Attach Service Routers
app.include_router(dynamic_option_chain_router)
app.include_router(scanner_router)
app.include_router(option_chain_router)
app.include_router(day_trader_router)
app.include_router(paper_oms_router)
app.include_router(backtester_router)
app.include_router(copy_trading_router)


@app.post("/api/feed/subscribe", tags=["Market Feed"])
def subscribe_feed_symbol(
    symbol: str = Query(..., description="Symbol, e.g. NIFTY25JAN2724000CE or RELIANCE"),
    token: str = Query(..., description="Token ID from ScripMaster"),
    exch_seg: str = Query(default="NSE", description="Exchange segment: NSE, NFO, BSE")
):
    """Subscribes an instrument on the live Angel One SmartWebSocketV2 feed upon selection."""
    success = feed_manager.subscribe_instrument(symbol, token, exch_seg)
    return {
        "status": "success" if success else "queued",
        "symbol": symbol,
        "token": token,
        "exch_seg": exch_seg,
        "message": f"Instrument {symbol} subscribed to live market feed."
    }


@app.websocket("/ws/stream")
async def websocket_stream_endpoint(websocket: WebSocket):
    """Real-time streaming WebSocket endpoint locked behind ADMIN_PIN query parameter."""
    pin_param = str(websocket.query_params.get("pin", "")).strip()
    if pin_param != ADMIN_PIN:
        await websocket.close(code=1008, reason="Unauthorized: Invalid or missing pin parameter")
        return

    await websocket.accept()
    ACTIVE_WS_CLIENTS.append(websocket)
    try:
        # Send initial snapshot immediately
        snapshot = json.dumps({
            "type": "snapshot",
            "timestamp": datetime.now().isoformat(),
            "live_prices": feed_manager.latest_prices,
            "market_state": feed_manager.market_state,
            "ticks_count": feed_manager.ticks_count,
            "mode": feed_manager.mode,
            "status": "connected",
        })
        await websocket.send_text(snapshot)

        async def heartbeat_loop():
            """Continuously broadcasts heartbeat frame every 20s to prevent Render load balancer disconnects."""
            while True:
                await asyncio.sleep(20)
                try:
                    await websocket.send_json({
                        "type": "heartbeat",
                        "timestamp": time.time(),
                        "status": "alive",
                    })
                except (WebSocketDisconnect, asyncio.CancelledError):
                    break
                except Exception:
                    break

        async def client_listener():
            """Listens for incoming client messages / ping frames cleanly."""
            while True:
                try:
                    msg = await websocket.receive_text()
                    if "ping" in msg.lower():
                        await websocket.send_json({"type": "pong", "timestamp": time.time()})
                except (WebSocketDisconnect, asyncio.CancelledError):
                    break
                except Exception:
                    break

        hb_task = asyncio.create_task(heartbeat_loop())
        listen_task = asyncio.create_task(client_listener())

        done, pending = await asyncio.wait(
            [hb_task, listen_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if websocket in ACTIVE_WS_CLIENTS:
            ACTIVE_WS_CLIENTS.remove(websocket)


# ==========================================================
# Root & Health Endpoints (Locked - Zero Public UI Exposure)
# ==========================================================
@app.api_route("/", methods=["GET", "HEAD"], tags=["General"])
@app.api_route("/health", methods=["GET", "HEAD"], tags=["General"])
async def locked_root_and_health(request: Request = None):
    """Locked root and health monitor endpoint. Exposes zero public UI or market data (keeps UptimeRobot green)."""
    return JSONResponse({"status": "locked", "service": "private"})


@app.api_route("/api/health", methods=["GET", "HEAD"], tags=["General"])
async def api_health_check(request: Request = None):
    """Health check for authenticated API clients."""
    if request and request.method == "HEAD":
        return JSONResponse({"status": "healthy"})
    return {
        "status": "healthy",
        "angel_logged_in": angel_auth.logged_in,
        "mode": feed_manager.mode,
        "last_tick_time": feed_manager.last_tick_time,
        "sample_reliance_ltp": feed_manager.latest_prices.get("RELIANCE"),
        "total_active_prices": len(feed_manager.latest_prices),
        "auth_error": angel_auth.last_error_msg,
    }


TERMINAL_HTML_PATH = os.path.join(CURRENT_DIR, "templates", "terminal.html")

@app.get("/owner", response_class=HTMLResponse, tags=["Owner Portal"])
@app.get("/admin", response_class=HTMLResponse, tags=["Owner Portal"])
@app.get("/portal", response_class=HTMLResponse, tags=["Owner Portal"])
@app.get("/terminal", response_class=HTMLResponse, tags=["Owner Portal"])
def owner_portal_page():
    """Private Owner Portal with dedicated PIN unlock section for authorized trading desk access."""
    if os.path.exists(TERMINAL_HTML_PATH):
        with open(TERMINAL_HTML_PATH, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Owner portal template not found</h1>", status_code=404)


@app.get("/api/debug/feed", tags=["General"])
def debug_feed():
    """Diagnostic feed endpoint verifying live Angel One connection and token quotes."""
    return {
        "angel_logged_in": angel_auth.logged_in,
        "last_auth_error": angel_auth.last_error_msg,
        "matched_eq_tokens_count": len(feed_manager.resolved_integer_tokens.get("NSE", [])),
        "live_fetched_count": len(feed_manager.latest_prices),
        "unmatched_symbols": getattr(feed_manager, "unmatched_symbols", []),
        "atherenerg": feed_manager.market_state.get("ATHERENERG"),
        "reliance": feed_manager.market_state.get("RELIANCE"),
        "sample_stocks": {
            k: feed_manager.market_state[k]
            for k in [
                "NIFTY", "RELIANCE", "HDFCBANK", "TCS", "SBIN",
                "ATHERENERG", "SWIGGY", "HYUNDAI", "M&M", "BAJAJ-AUTO", "ETERNAL"
            ]
            if k in feed_manager.market_state
        },
    }


@app.get("/api", tags=["General"])
def api_endpoints_schema():
    return {
        "name": "Indian Algo Trader Backend",
        "version": "2.0.0",
        "status": "online",
        "market": "National Stock Exchange (NSE) & Bombay Stock Exchange (BSE) of India",
        "endpoints": {
            "copy_trade_status": "/api/copy-trade/status",
            "copy_trade_execute": "/api/copy-trade/execute",
            "copy_trade_flatten_all": "/api/copy-trade/flatten-all",
            "option_chain": "/api/option-chain?symbol=NIFTY&expiry=nearest",
            "stock_search": "/api/market/search?q=RELIANCE",
            "stock_detail": "/api/market/stock?symbol=RELIANCE",
            "orb_scanner": "/api/trader/scanner?strategy=orb",
            "entry_evaluator": "/api/trader/entry",
            "exit_planner": "/api/trader/exit",
            "risk_manager": "/api/trader/risk",
            "timemachine": "/api/trader/timemachine",
            "paper_oms_orders": "/api/paper/order",
            "paper_oms_positions": "/api/paper/positions",
            "paper_oms_portfolio": "/api/paper/portfolio",
            "optopsy_simulate": "/api/optopsy-simulate",
            "health": "/health",
        },
    }


@app.get("/api/state", tags=["General"])
def get_system_state():
    """Returns current live system state, market hours, virtual paper OMS portfolio and prices."""
    now = datetime.now()
    uptime = int(time.time() - APP_START_TIME)
    
    # NSE market hours: Mon-Fri 09:15 to 15:30 IST
    is_weekday = now.weekday() < 5
    is_market_hours = is_weekday and ((now.hour == 9 and now.minute >= 15) or (10 <= now.hour < 15) or (now.hour == 15 and now.minute <= 30))

    portfolio = paper_engine.get_portfolio_summary()

    with paper_engine.lock:
        live_prices = {**feed_manager.latest_prices, **paper_engine.live_prices}
        active_positions = [
            t.model_dump() for t in paper_engine.trades.values() if t.status == "OPEN"
        ]

    market_info = {
        "exchange": "NSE / BSE India",
        "is_open": is_market_hours,
        "session": "Regular (09:15-15:30 IST)" if is_market_hours else "Closed",
        "opening_range_window": "09:15 - 09:30 IST",
        "flat_by_time": "15:20 IST",
    }

    # Dynamic calculation of LTP changes, point differences, and %Chg
    ticker_data = {}
    for sym, ltp in live_prices.items():
        st = feed_manager.market_state.get(sym, {})
        prev_close = st.get("prev_close") or feed_manager.previous_closes.get(sym) or PREVIOUS_CLOSES.get(sym, ltp)
        chg = st.get("change") if st.get("change") is not None else round(ltp - prev_close, 2)
        chg_pct = st.get("percent_change") if st.get("percent_change") is not None else (round((chg / prev_close) * 100, 2) if prev_close > 0 else 0.0)
        ticker_data[sym] = {
            "symbol": sym,
            "ltp": ltp,
            "current_price": ltp,
            "prev_close": prev_close,
            "previous_close": prev_close,
            "close": prev_close,
            "change": chg,
            "net_change": chg,
            "netChange": chg,
            "pct_change": chg_pct,
            "change_pct": chg_pct,
            "percent_change": chg_pct,
            "percentChange": chg_pct,
            "direction": "up" if chg > 0 else ("down" if chg < 0 else "neutral"),
        }

    return {
        "status": "ok",
        "timestamp": now.isoformat(),
        "uptime_seconds": uptime,
        "market": market_info,
        "market_data": market_info,
        "live_prices": live_prices,
        "ticker_data": ticker_data,
        "previous_closes": PREVIOUS_CLOSES,
        "feed": {
            "provider": "Angel One SmartWebSocketV2",
            "mode": feed_manager.mode,
            "connected": feed_manager.running,
            "account_holder": feed_manager.user_name,
            "ticks_streamed": feed_manager.ticks_count,
            "last_tick_time": feed_manager.last_tick_time,
            "symbol_token_lookup": feed_manager.symbol_to_token,
            "resolved_integer_tokens": feed_manager.resolved_integer_tokens,
            "active_symbols": list(live_prices.keys()),
            "latest_prices": live_prices,
        },
        "paper_oms": {
            "mode": "100% Virtual Paper Trading",
            "live_broker_execution": "Strictly Forbidden (placeOrder disabled)",
            "portfolio": portfolio,
            "active_positions_count": len(active_positions),
            "active_positions": active_positions,
        },
    }


if __name__ == "__main__":
    import uvicorn
    # Hugging Face Spaces port is 7860
    port = int(os.getenv("PORT", 7860))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
