# Indian Algo Trader — Production FastAPI Backend

A production-grade Python 3.12 algorithmic trading engine tailored for the **National Stock Exchange (NSE)** and **Bombay Stock Exchange (BSE)** of India.

Designed for high-performance cloud deployment on **Hugging Face Spaces** (Port `7860`, non-root user `1000`) or standard Docker/Kubernetes environments.

---

## 🏛️ Architecture & Repositories Integration

```
indian-algo-trader/
├── repos/
│   ├── Python-NSE-Option-Chain-Analyzer/   # Reverse-engineered NSE warmup & OI indicators
│   ├── sensibull-realtime-options-api-ingestor/ # Standardized options JSON data schema
│   ├── Indian-Stock-Market-API/            # Yahoo Finance NSE/BSE quote & search engine
│   ├── day-trader-skill/                   # 15-min ORB, VWAP, ATR SL, Monte Carlo replays
│   └── optopsy/                            # Options backtesting engine (singles, spreads, condors)
└── backend/
    ├── main.py                             # FastAPI app + Angel One SmartWebSocketV2 background feed
    ├── requirements.txt                    # Python 3.12 production dependencies
    ├── Dockerfile                          # Port 7860, non-root user 1000 (Hugging Face Spaces ready)
    ├── README.md                           # Complete documentation & API catalog
    └── services/
        ├── option_chain_service.py         # Headless NSE Option Chain Engine
        ├── day_trader_nse.py               # NSE Scanner, ORB, Entry/Exit, Risk & Time Machine
        ├── paper_oms.py                    # 100% Virtual Paper Trading OMS (Real broker prohibited)
        └── backtester.py                   # Optopsy Options Strategy Backtester
```

---

## 🚀 Key Modules & Services

### 1. Headless NSE Option Chain Engine (`backend/services/option_chain_service.py`)
- **Session Warmup:** Performs automatic HTTP cookie handshakes on `https://www.nseindia.com` and `/option-chain`.
- **Headless Execution:** All GUI dependencies (`tkinter`, `tksheet`, `configparser`, `win10toast`) have been completely stripped.
- **Exact OI Indicator Math:**
  - `call_sum`, `put_sum`, `difference`
  - `call_boundary`, `put_boundary`
  - `call_itm`, `put_itm`
  - Put-Call Ratio (`pcr`)
  - `call_itm_signal` & `put_itm_signal`
  - `call_writer_exits` & `put_writer_exits`
- **Output Schema:** Conforms strictly to the Sensibull Realtime Options API Ingestor structure.
- **Endpoint:** `GET /api/option-chain?symbol=NIFTY&expiry=nearest`

### 2. Indian Stock Market & Day Trader Strategy Engine (`backend/services/day_trader_nse.py`)
- **Native FastAPI Market Endpoints:**
  - `GET /api/market/search?q={query}`: Fast autocomplete searching NSE symbols with local cache and Yahoo Finance fallback.
  - `GET /api/market/stock?symbol={symbol}&res={val|num}`: Stock details (LTP, Day High/Low, 52W High/Low, Volume, etc.).
  - `GET /api/market/stock/list?symbols={sym1,sym2}`: High-throughput batch quotes.
- **NSE Day Trader Logic (Adapted for 09:15–15:30 IST):**
  - `GET /api/trader/scanner`: 15-min Opening Range Breakout (ORB) for 09:15–09:30 IST and daily setups (`breakout_20d`, `bull_flag`, `reclaim_ma20`, `oversold`).
  - `POST /api/trader/entry`: Validates ORB trigger with VWAP, Volume (>1.2x range average), and Option Chain PCR/ITM filters.
  - `POST /api/trader/exit`: Plans 1x ATR Stop Loss (rounded down to 0.05 tick size), 1.5R and 2.5R target levels, with an automated flat-by time of 15:20 IST.
  - `POST /api/trader/risk`: Sizes positions strictly against virtual account capital and max loss per trade; enforces reward-risk checks (>= 1.5R).
  - `POST /api/trader/timemachine`: Runs 2,000–10,000 Monte Carlo replays using 60-day 5-min historical bars for NSE equities (`.NS`), estimating target vs stop probabilities.

### 3. 100% Virtual Paper Trading OMS (`backend/services/paper_oms.py`)
- **Strict Safety Directive:** Live broker order execution is strictly forbidden (`placeOrder` is never imported, defined, or executed).
- **Virtual Order Execution:** Orders are executed in a virtual wallet (default ₹10,00,000 capital).
- **Supabase Persistence:** Logs all paper trades into a Supabase `paper_trades` table (with thread-safe in-memory cache fallback).
- **Live Unrealized P&L & Auto-Exit:** Evaluates live LTP against incoming Angel One WebSocket ticks; automatically triggers stop-loss and profit targets.
- **Telegram Copy-Trade Alerts:** Dispatches real-time order alerts to Telegram channels via Telegram Bot API.
- **Endpoints:**
  - `POST /api/paper/order`: Place paper trade
  - `GET /api/paper/positions`: List active and historical positions with live P&L
  - `POST /api/paper/close/{trade_id}`: Manually close a trade
  - `GET /api/paper/portfolio`: Virtual wallet balance, realized/unrealized P&L, win rate
  - `POST /api/paper/simulate-tick`: Test tick processing and SL triggers

### 4. Optopsy Options Backtester (`backend/services/backtester.py`)
- Bridges `repos/optopsy` into FastAPI.
- Normalizes NSE option chain snapshots into Optopsy's schema (`underlying_symbol`, `option_type`, `expiration`, `quote_date`, `strike`, `bid`, `ask`, `delta`, `underlying_price`).
- Simulates options strategies: `long_calls`, `long_puts`, `bull_call_spread`, `bear_put_spread`, `iron_condor`, `straddle`, `strangle`.
- Provides performance metrics: Win Rate, Net P&L, Profit Factor, Max Drawdown, Sharpe Ratio, and trade log.
- **Endpoint:** `POST /api/optopsy-simulate`

### 5. Angel One SmartWebSocketV2 Background Feed (`backend/main.py`)
- Automatically spawns a background thread on startup.
- Connects to official `SmartWebSocketV2` when Angel One credentials are provided.
- Gracefully falls back to an autonomous tick simulation generator when credentials are omitted, ensuring paper OMS and P&L monitors execute 24/7.

---

## 🛠️ Environment Variables Configuration

Create a `.env` file or configure secrets in Hugging Face Spaces:

```env
# Server Configuration
PORT=7860

# Supabase Database (Optional for cloud paper trades table)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-or-service-role-key

# Telegram Copy-Trade Bot Alerts (Optional)
TELEGRAM_BOT_TOKEN=123456789:ABCDefghIJKlmnoPQRstuvWXYZ
TELEGRAM_CHAT_ID=-1001234567890

# Angel One SmartAPI WebSocket Feed (Optional)
SMARTAPI_API_KEY=your_smartapi_api_key
SMARTAPI_CLIENT_CODE=your_client_code
SMARTAPI_PASSWORD=your_password
SMARTAPI_TOTP_KEY=your_totp_key
SMARTAPI_FEED_TOKEN=your_feed_token
```

---

## 🐳 Docker & Hugging Face Spaces Deployment

### Run Locally with Docker
```bash
docker build -t indian-algo-trader -f backend/Dockerfile .
docker run -p 7860:7860 indian-algo-trader
```

### Deploy to Hugging Face Spaces
1. Create a new Space on [Hugging Face](https://huggingface.co/spaces) with **Docker** SDK.
2. Push this repository to your Space repository.
3. Hugging Face Spaces will automatically build the `Dockerfile`, run as non-root user `1000`, and bind to port `7860`.
4. Access interactive Swagger UI documentation at:
   `https://your-space.hf.space/docs`
