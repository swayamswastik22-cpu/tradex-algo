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
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, Request
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
PREVIOUS_CLOSES: Dict[str, float] = {
    "NIFTY": 23440.8,
    "BANKNIFTY": 55002.2,
    "FINNIFTY": 24050.0,
    "MIDCPNIFTY": 12850.0,
}
INITIAL_LTP_PRICES: Dict[str, float] = {
    "NIFTY": 23810.85,
    "BANKNIFTY": 55540.05,
    "FINNIFTY": 24200.0,
    "MIDCPNIFTY": 12850.0,
}

for data_path in DATA_FILE_CANDIDATES:
    if os.path.exists(data_path):
        try:
            with open(data_path, "r", encoding="utf-8") as f:
                loaded_stocks = json.load(f)
                for s in loaded_stocks:
                    sym = s["symbol"].upper().strip()
                    if sym not in ALL_STOCK_SYMBOLS:
                        ALL_STOCK_SYMBOLS.append(sym)
                    if s.get("prev_close"):
                        PREVIOUS_CLOSES[sym] = float(s["prev_close"])
                    if s.get("ltp"):
                        INITIAL_LTP_PRICES[sym] = float(s["ltp"])
            logger.info(f"Loaded {len(ALL_STOCK_SYMBOLS)} FNO stocks from {data_path} into universe and pricing state.")
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


class AngelOneWebSocketManager:
    """Manages live market tick consumption from Angel One SmartWebSocketV2

    with automatic TOTP session initialization, OpenAPIScripMaster token extraction,
    and streaming to Virtual Paper OMS and global state.
    """

    def __init__(self):
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.sws: Optional[Any] = None
        self.api_key = os.getenv("ANGEL_API_KEY") or os.getenv("SMARTAPI_API_KEY", "")
        self.client_code = os.getenv("ANGEL_CLIENT_CODE") or os.getenv("SMARTAPI_CLIENT_CODE", "")
        self.pin = os.getenv("ANGEL_PIN") or os.getenv("SMARTAPI_PASSWORD", "")
        self.totp_secret = os.getenv("ANGEL_TOTP_SECRET") or os.getenv("SMARTAPI_TOTP_KEY", "")
        self.user_name = ""
        self.ticks_count = 0
        self.last_tick_time = ""
        self.latest_prices: Dict[str, float] = dict(INITIAL_LTP_PRICES)
        self.market_state: Dict[str, Dict[str, Any]] = {}
        self.nfo_contracts_by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.previous_closes: Dict[str, float] = dict(PREVIOUS_CLOSES)
        self.token_to_symbol: Dict[str, str] = dict(TOKEN_TO_SYMBOL)
        self.symbol_to_token: Dict[str, int] = {}
        self.resolved_integer_tokens: Dict[str, List[int]] = {"NSE": [], "INDICES": []}
        self.mode = (
            "LIVE (Angel One SmartWebSocketV2)"
            if (self.api_key and self.client_code and self.pin and self.totp_secret)
            else "SIMULATED"
        )

        # Initialize real-time state for universe
        for sym in (ALL_STOCK_SYMBOLS + ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]):
            p_close = PREVIOUS_CLOSES.get(sym, 1000.0)
            init_p = INITIAL_LTP_PRICES.get(sym, p_close)
            self.market_state[sym] = {
                "symbol": sym,
                "ltp": init_p,
                "high": max(init_p, p_close),
                "low": min(init_p, p_close),
                "open": init_p,
                "prev_close": p_close,
                "change": round(init_p - p_close, 2),
                "percent_change": round(((init_p - p_close) / p_close) * 100, 2) if p_close > 0 else 0.0,
                "orb_high": round(p_close * 1.006, 2),
                "orb_low": round(p_close * 0.994, 2),
                "volume": 0,
                "last_tick_time": "",
            }

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True, name="AngelOneFeedThread")
        self.thread.start()
        logger.info(f"Angel One Market Feed started in {self.mode} mode.")

    def stop(self):
        self.running = False

    def _fetch_scrip_master(self) -> Tuple[List[int], List[int], Dict[str, str], Dict[str, int]]:
        """Downloads Angel One OpenAPIScripMaster.json and builds fast dictionary lookup

        for all 218 FNO stocks plus NIFTY, BANKNIFTY, and FINNIFTY indices.
        """
        target_equities = set(ALL_STOCK_SYMBOLS) if ALL_STOCK_SYMBOLS else {
            "RELIANCE", "HDFCBANK", "TCS", "INFY", "SBIN", "ICICIBANK", "LT", "ITC"
        }
        
        nse_equity_tokens: List[int] = []
        index_tokens: List[int] = []
        token_to_symbol: Dict[str, str] = dict(TOKEN_TO_SYMBOL)
        symbol_to_token: Dict[str, int] = {}

        local_cache = os.path.join(CURRENT_DIR, "OpenAPIScripMaster.json")
        instruments = None

        if os.path.exists(local_cache):
            try:
                with open(local_cache, "r", encoding="utf-8") as f:
                    instruments = json.load(f)
                logger.info(f"Loaded {len(instruments)} instruments from local ScripMaster cache.")
            except Exception as e:
                logger.warning(f"Failed to read local cache: {e}")

        if not instruments:
            urls = [
                "https://margincalculator.angelbroking.com/OpenAPI_File/OpenAPIScripMaster.json",
                "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
                "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json",
            ]
            for url in urls:
                try:
                    logger.info(f"Downloading ScripMaster from: {url}")
                    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
                    if resp.status_code == 200 and "application/json" in resp.headers.get("content-type", ""):
                        instruments = resp.json()
                        with open(local_cache, "w", encoding="utf-8") as f:
                            json.dump(instruments, f)
                        logger.info(f"Downloaded and cached {len(instruments)} instruments from {url}.")
                        break
                except Exception as ex:
                    logger.warning(f"Error fetching {url}: {ex}")

        if instruments:
            for item in instruments:
                sym = str(item.get("symbol", "")).upper()
                token_raw = item.get("token")
                if not token_raw:
                    continue
                try:
                    token_int = int(token_raw)
                except (ValueError, TypeError):
                    continue

                exch = item.get("exch_seg")
                name = str(item.get("name", "")).upper()
                itype = str(item.get("instrumenttype", ""))

                # Match Equities in NSE
                clean_sym = sym.replace("-EQ", "")
                if exch == "NSE" and clean_sym in target_equities and itype in ["", "EQ"]:
                    if token_int not in nse_equity_tokens:
                        nse_equity_tokens.append(token_int)
                    token_to_symbol[str(token_int)] = clean_sym
                    symbol_to_token[clean_sym] = token_int

                # Match Indices (exchangeType 3 / NSE index tokens)
                if exch == "NSE" and (sym in ["NIFTY", "NIFTY 50", "BANKNIFTY", "NIFTY BANK"] or (itype == "AMXIDX" and name in ["NIFTY", "BANKNIFTY"])):
                    matched_idx = "NIFTY" if "NIFTY" in sym and "BANK" not in sym else "BANKNIFTY"
                    if token_int not in index_tokens:
                        index_tokens.append(token_int)
                    token_to_symbol[str(token_int)] = matched_idx
                    symbol_to_token[matched_idx] = token_int

                # Match Active NFO Contracts (Futures & Options) for all 218 FNO stocks & indices
                if exch == "NFO":
                    clean_name = name.replace("-EQ", "").strip().upper()
                    if clean_name in target_equities or clean_name in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
                        self.nfo_contracts_by_symbol[clean_name].append(item)

        # Guaranteed fallbacks for exact integer tokens
        default_map = {
            2885: "RELIANCE",
            1333: "HDFCBANK",
            11536: "TCS",
            1594: "INFY",
            3045: "SBIN",
            4963: "ICICIBANK",
            11483: "LT",
            1660: "ITC",
            26000: "NIFTY",
            26009: "BANKNIFTY",
            99926000: "NIFTY",
            99926009: "BANKNIFTY",
        }
        for tok, sym in default_map.items():
            token_to_symbol[str(tok)] = sym
            if sym not in symbol_to_token:
                symbol_to_token[sym] = tok
            if tok in [26000, 26009, 99926000, 99926009]:
                if tok not in index_tokens:
                    index_tokens.append(tok)
            else:
                if tok not in nse_equity_tokens:
                    nse_equity_tokens.append(tok)

        return nse_equity_tokens, index_tokens, token_to_symbol, symbol_to_token

    def _run_loop(self):
        if "LIVE" in self.mode:
            self._connect_live_smart_websocket()
        else:
            self._run_simulated_tick_feed()

    def _connect_live_smart_websocket(self):
        """Authenticates via SmartAPI TOTP session and streams live binary ticks via SmartWebSocketV2."""
        while self.running:
            try:
                import pyotp
                from SmartApi import SmartConnect
                from SmartApi.smartWebSocketV2 import SmartWebSocketV2

                logger.info("Authenticating with Angel One SmartAPI using TOTP...")
                totp = pyotp.TOTP(self.totp_secret).now()
                smart_api = SmartConnect(api_key=self.api_key)
                session = smart_api.generateSession(self.client_code, self.pin, totp)

                if not session.get("status"):
                    logger.warning(f"Angel One session failed: {session.get('message')}. Retrying in 10s...")
                    time.sleep(10)
                    continue

                session_data = session.get("data", {})
                jwt_token = session_data.get("jwtToken")
                feed_token = session_data.get("feedToken")
                self.user_name = session_data.get("name", "Angel One Client")
                logger.info(f"Angel One Authenticated for: {self.user_name} ({self.client_code})")

                # Build fast dictionary lookup and resolved integer tokens
                equity_tokens, idx_tokens, token_map, sym_map = self._fetch_scrip_master()
                self.token_to_symbol = token_map
                self.symbol_to_token = sym_map
                self.resolved_integer_tokens = {"NSE": equity_tokens, "INDICES": idx_tokens}
                logger.info(
                    f"Resolved Token Lookup: {len(sym_map)} symbols, {len(equity_tokens)} Equity tokens, {len(idx_tokens)} Index tokens."
                )

                sws = SmartWebSocketV2(
                    auth_token=jwt_token,
                    api_key=self.api_key,
                    client_code=self.client_code,
                    feed_token=feed_token,
                    retry_strategy=1,
                    retry_delay=5,
                    max_retry_attempt=15,
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
                        # Last traded price in Angel One binary ticks is given in paise (100 paise = 1 INR)
                        ltp = float(raw_ltp) / 100.0 if raw_ltp > 0 else 0.0
                        sym = self.token_to_symbol.get(token) or TOKEN_TO_SYMBOL.get(token, f"NSE_{token}")

                        if ltp > 0:
                            price_val = round(ltp, 2)
                            now_dt = datetime.now()
                            now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
                            now_iso = now_dt.isoformat()
                            now_ts = now_dt.timestamp()

                            # Print received tick to console log
                            print(f"[TICK] {sym:<10} (Token: {token:<8}) -> LTP: Rs {price_val:>9.2f} | Time: {now_str}", flush=True)

                            # Update manager state & global dictionary immediately
                            self.ticks_count += 1
                            self.last_tick_time = now_iso
                            self.latest_prices[sym] = price_val

                            # Update market_state for real-time live scanner & candle engine
                            st = self.market_state.get(sym)
                            prev_close = self.previous_closes.get(sym, price_val)
                            if st is None:
                                st = {
                                    "symbol": sym,
                                    "ltp": price_val,
                                    "high": price_val,
                                    "low": price_val,
                                    "open": price_val,
                                    "prev_close": prev_close,
                                    "change": round(price_val - prev_close, 2),
                                    "percent_change": round(((price_val - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0.0,
                                    "orb_high": round(prev_close * 1.006, 2),
                                    "orb_low": round(prev_close * 0.994, 2),
                                    "volume": 0,
                                    "last_tick_time": now_iso,
                                }
                                self.market_state[sym] = st
                            else:
                                st["ltp"] = price_val
                                st["high"] = max(st.get("high", price_val), price_val)
                                st["low"] = min(st.get("low", price_val), price_val)
                                chg = round(price_val - prev_close, 2)
                                st["change"] = chg
                                st["percent_change"] = round((chg / prev_close) * 100, 2) if prev_close > 0 else 0.0
                                st["last_tick_time"] = now_iso

                                # Live Opening Range Window (09:15 - 09:30 IST)
                                if now_dt.hour == 9 and 15 <= now_dt.minute <= 30:
                                    st["orb_high"] = max(st.get("orb_high", price_val), price_val)
                                    st["orb_low"] = min(st.get("orb_low", price_val), price_val)

                            # Ingest into Multi-Timeframe Candlestick Engine
                            candle_engine.add_tick(sym, price_val, now_ts)

                            # Update Virtual Paper OMS
                            paper_engine.process_tick(sym, price_val)
                    except Exception as ex:
                        logger.error(f"Tick parse error: {ex}")

                def on_open(wsapp):
                    logger.info("Angel One SmartWebSocketV2 connected! Subscribing with resolved tokens...")
                    print(
                        f"[WS OPEN] Subscribing {len(equity_tokens)} equities (exchangeType 1) "
                        f"and {len(idx_tokens)} indices (exchangeType 3 & 1)...",
                        flush=True,
                    )
                    # Indices like NIFTY/BANKNIFTY are sent with exchangeType 3 (and 1), Equities like RELIANCE use exchangeType 1
                    token_list = [
                        {"exchangeType": 1, "tokens": [str(t) for t in equity_tokens]},
                        {"exchangeType": 3, "tokens": [str(t) for t in idx_tokens]},
                        {"exchangeType": 1, "tokens": [str(t) for t in idx_tokens]},
                    ]
                    sws.subscribe("algo_feed_sub", 1, token_list)

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

    def _run_simulated_tick_feed(self):
        """Fallback simulator for when live credentials are not set."""
        base_prices = dict(self.latest_prices)
        while self.running:
            try:
                now_ts = time.time()
                for sym, cur in list(base_prices.items()):
                    step = cur * 0.0004 * random.choice([-1.0, 0.0, 1.0])
                    new_price = round(cur + step, 2)
                    base_prices[sym] = new_price
                    self.latest_prices[sym] = new_price

                    st = self.market_state.get(sym)
                    if st:
                        st["ltp"] = new_price
                        st["high"] = max(st.get("high", new_price), new_price)
                        st["low"] = min(st.get("low", new_price), new_price)
                        prev = st.get("prev_close", new_price)
                        chg = round(new_price - prev, 2)
                        st["change"] = chg
                        st["percent_change"] = round((chg / prev) * 100, 2) if prev > 0 else 0.0

                    candle_engine.add_tick(sym, new_price, now_ts)
                    paper_engine.process_tick(sym, new_price)
                time.sleep(2)
            except Exception:
                time.sleep(2)

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


# Active WebSocket connections to /ws/stream
ACTIVE_WS_CLIENTS: List[WebSocket] = []


async def broadcast_ws_loop():
    """Broadcasts real-time prices to all active /ws/stream clients every 500ms."""
    while True:
        try:
            await asyncio.sleep(0.5)
            if not ACTIVE_WS_CLIENTS:
                continue

            payload = json.dumps({
                "type": "tick_stream",
                "timestamp": datetime.now().isoformat(),
                "live_prices": feed_manager.latest_prices,
                "ticks_count": feed_manager.ticks_count,
                "mode": feed_manager.mode,
                "status": "connected"
            })

            dead_clients = []
            for client in ACTIVE_WS_CLIENTS:
                try:
                    await client.send_text(payload)
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

# CORS Middleware (allowing AI Studio frontend and localhost)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://tradex-algo-trading-desk.ai.studio",
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:7860",
        "https://tradex-algo.onrender.com",
    ],
    allow_origin_regex=r"https://.*\.ai\.studio",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    """Real-time streaming WebSocket endpoint for UI clients."""
    await websocket.accept()
    ACTIVE_WS_CLIENTS.append(websocket)
    try:
        # Send initial snapshot immediately
        snapshot = json.dumps({
            "type": "snapshot",
            "timestamp": datetime.now().isoformat(),
            "live_prices": feed_manager.latest_prices,
            "ticks_count": feed_manager.ticks_count,
            "mode": feed_manager.mode,
            "status": "connected"
        })
        await websocket.send_text(snapshot)
        while True:
            # Keep alive and handle client message pings
            _ = await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if websocket in ACTIVE_WS_CLIENTS:
            ACTIVE_WS_CLIENTS.remove(websocket)


# ==========================================================
# Root & Health Endpoints (Supporting HEAD for UptimeRobot)
# ==========================================================
TERMINAL_HTML_PATH = os.path.join(CURRENT_DIR, "templates", "terminal.html")

@app.api_route("/", methods=["GET", "HEAD"], tags=["General"])
async def root(request: Request = None, format: Optional[str] = Query(default=None)):
    """Handles GET and HEAD for root route (UptimeRobot monitor & Pro Trading Desk)."""
    if request and request.method == "HEAD":
        return JSONResponse({"status": "online"})
    accept_header = request.headers.get("accept", "") if request else ""
    if format != "json" and ("text/html" in accept_header or "*/*" in accept_header):
        if os.path.exists(TERMINAL_HTML_PATH):
            with open(TERMINAL_HTML_PATH, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
    return {"status": "online"}


@app.api_route("/health", methods=["GET", "HEAD"], tags=["General"])
@app.api_route("/api/health", methods=["GET", "HEAD"], tags=["General"])
async def health_check():
    """Handles GET and HEAD for health checks (UptimeRobot monitor)."""
    return {"status": "healthy"}


@app.get("/terminal", response_class=HTMLResponse, tags=["General"])
def terminal_page():
    """Serves the rich interactive TRADEx-ALGO Live Terminal & Copy Trader."""
    if os.path.exists(TERMINAL_HTML_PATH):
        with open(TERMINAL_HTML_PATH, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Terminal HTML not found</h1>", status_code=404)


@app.get("/api", tags=["General"])
def api_endpoints_schema():
    return {
        "name": "Indian Algo Trader Backend",
        "version": "2.0.0",
        "status": "online",
        "market": "National Stock Exchange (NSE) & Bombay Stock Exchange (BSE) of India",
        "docs_url": "/docs",
        "terminal_url": "/terminal",
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
        prev_close = PREVIOUS_CLOSES.get(sym, ltp)
        chg = round(ltp - prev_close, 2)
        chg_pct = round((chg / prev_close) * 100, 2) if prev_close > 0 else 0.0
        ticker_data[sym] = {
            "symbol": sym,
            "ltp": ltp,
            "prev_close": prev_close,
            "change": chg,
            "change_pct": chg_pct,
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
