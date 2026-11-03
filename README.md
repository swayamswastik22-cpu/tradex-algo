# TRADEx-ALGO 🚀

> High-performance algorithmic trading and market analytics platform for Indian National Stock Exchange (NSE) & Bombay Stock Exchange (BSE).

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)](https://fastapi.tiangolo.com)
[![Angel One SmartAPI](https://img.shields.io/badge/Angel%20One-SmartWebSocketV2-orange.svg)](https://smartapi.angelbroking.com)
[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com)

---

## 🌟 Key Features

1. **Headless NSE Option Chain Engine (`/api/option-chain`)**
   - 100% GUI-independent headless NSE session warmup and real-time cookie handling.
   - Computes Open Interest (OI) math: `call_sum`, `put_sum`, `call_boundary`, `put_boundary`, `call_itm`, `put_itm`, `pcr`, and writer exit signals.
   - Conforms to Sensibull-standard real-time JSON format.

2. **Indian Market Scanner & 15-Min ORB Day Trader (`/api/market/*`, `/api/trader/*`)**
   - Native FastAPI endpoints for stock search, details, and lists.
   - 15-minute Opening Range Breakout (ORB) strategy (09:15–09:30 IST) with VWAP, volume spikes, and Option Chain PCR/ITM confirmations.
   - Automated exit planning: 1x ATR Stop Loss, 1.5R and 2.5R Profit Targets, and strict 15:20 IST "flat-by" intraday closure.
   - 60-day 5-minute bar Monte Carlo Time Machine simulation.

3. **100% Virtual Paper Trading OMS (`/api/paper/*`)**
   - Live broker order execution (`placeOrder`) is strictly disabled for risk-free simulation.
   - Real-time P&L tracking against live ticks.
   - Auto-closes trades on SL/Target triggers.
   - Instant broadcast alerts via Telegram Bot API and optional Supabase cloud storage.

4. **Universal Symbol & FNO Search Bar (`/api/market/scrip-search`)**
   - In-memory lookups across 150,000+ instruments from Angel One `OpenAPIScripMaster.json`.
   - Autocomplete for Equities, Futures (`FUT`), and Options (`CE`/`PE`).
   - Dynamic WebSocket subscription endpoint (`POST /api/feed/subscribe`).

5. **Dynamic Candlestick Engine (`/api/market/candles`)**
   - Real-time multi-timeframe charts (`1m`, `3m`, `5m`, `15m`, `1D`).
   - Resamples 1-minute bars into 3-minute intervals.
   - Injects incoming live WebSocket ticks directly into the active candle in real time.

---

## 🚀 Quickstart

### Local Setup

```bash
# 1. Clone the repository
git clone https://github.com/swayamswastik22-cpu/tradex-algo.git
cd tradex-algo/backend

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment variables (optional)
cp .env.example .env

# 4. Start the FastAPI server
uvicorn main:app --host 0.0.0.0 --port 7860 --reload
```

Interactive API documentation available at `http://localhost:7860/docs`.

---

## ☁️ Deployment on Render

1. Create a new **Web Service** on [Render Dashboard](https://dashboard.render.com).
2. Connect your `tradex-algo` GitHub repository.
3. Set the following settings:
   - **Root Directory**: `backend`
   - **Environment**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
4. Add environment variables in the Render Dashboard (from `.env.example`).
5. Click **Create Web Service**.

Alternatively, connect with Render Blueprints using [`render.yaml`](./render.yaml).

---

## 🔒 Security

- Real broker execution (`placeOrder`) is strictly forbidden by code design.
- Credentials and private secrets are managed via environment variables and excluded via `.gitignore`.
- Wildcard CORS is configured with `allow_credentials=False` for strict browser security compliance.
