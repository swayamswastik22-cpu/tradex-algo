"""2026 Indian Stock Market Brokerage, Tax & Friction Accounting Engine.

Computes exact real-world friction for simulated, paper, and live trades in Indian markets:
1. Slippage: 0.5% on option premium + Rs 0.05 tick per leg; 0.02% on futures/equity turnover.
2. Brokerage: Flat Rs 20 per executed order (Rs 40 round-trip).
3. Securities Transaction Tax (STT - 2026 Rules):
   - Options: 0.1% on Sell turnover (premium value, updated budget rate).
   - Futures: 0.02% on Sell turnover.
   - Equity Intraday: 0.025% on Sell turnover.
4. Exchange Transaction Charges (NSE):
   - Options: 0.05% on premium turnover.
   - Futures: 0.0019% on turnover.
   - Equity: 0.00325% on turnover.
5. SEBI Turnover Fees: Rs 10 per crore (0.0001% of turnover).
6. Stamp Duty:
   - Options: 0.003% on Buy premium turnover.
   - Futures: 0.002% on Buy turnover.
   - Equity: 0.003% on Buy turnover.
7. GST: 18% on (Brokerage + Exchange Charges + SEBI Fees).
"""

from typing import Dict, Any


def calculate_trade_friction(
    instrument_type: str,
    entry_price: float,
    exit_price: float,
    qty: int,
    is_buy_first: bool = True,
) -> Dict[str, Any]:
    """Calculates granular real-world Indian market transaction friction for a round-trip trade."""
    inst = instrument_type.upper().strip()
    qty = max(abs(int(qty)), 1)
    entry_price = max(float(entry_price), 0.01)
    exit_price = max(float(exit_price), 0.01)

    buy_price = entry_price if is_buy_first else exit_price
    sell_price = exit_price if is_buy_first else entry_price

    buy_turnover = buy_price * qty
    sell_turnover = sell_price * qty
    total_turnover = buy_turnover + sell_turnover

    # 1. Slippage calculation
    if "OPT" in inst:
        # Options slippage: 0.5% premium slippage + 0.05 tick per leg
        buy_slippage = (buy_price * 0.005 + 0.05) * qty
        sell_slippage = (sell_price * 0.005 + 0.05) * qty
    else:
        # Futures & Equity intraday slippage: 0.02% on turnover
        buy_slippage = buy_turnover * 0.0002
        sell_slippage = sell_turnover * 0.0002
    total_slippage = round(buy_slippage + sell_slippage, 2)

    # 2. Brokerage: Flat Rs 20 per executed order leg
    brokerage = 40.0

    # 3. Securities Transaction Tax (STT - 2026)
    if "OPT" in inst:
        # STT on Options: 0.1% on Sell turnover
        stt = round(sell_turnover * 0.001, 2)
    elif "FUT" in inst:
        # STT on Futures: 0.02% on Sell turnover
        stt = round(sell_turnover * 0.0002, 2)
    else:
        # Equity Intraday: 0.025% on Sell turnover
        stt = round(sell_turnover * 0.00025, 2)

    # 4. Exchange Transaction Charges (NSE)
    if "OPT" in inst:
        exchange_charges = round(total_turnover * 0.0005, 2)
    elif "FUT" in inst:
        exchange_charges = round(total_turnover * 0.000019, 2)
    else:
        exchange_charges = round(total_turnover * 0.0000325, 2)

    # 5. SEBI Turnover Fees: Rs 10 per crore (0.0001%)
    sebi_charges = round(total_turnover * 0.000001, 2)
    if sebi_charges < 0.01:
        sebi_charges = 0.01

    # 6. Stamp Duty (on Buy leg only)
    if "OPT" in inst:
        stamp_duty = round(buy_turnover * 0.00003, 2)
    elif "FUT" in inst:
        stamp_duty = round(buy_turnover * 0.00002, 2)
    else:
        stamp_duty = round(buy_turnover * 0.00003, 2)

    # 7. GST: 18% on (Brokerage + Exchange Charges + SEBI Charges)
    gst_base = brokerage + exchange_charges + sebi_charges
    gst = round(gst_base * 0.18, 2)

    total_taxes_and_charges = round(brokerage + stt + exchange_charges + sebi_charges + stamp_duty + gst, 2)
    total_friction = round(total_taxes_and_charges + total_slippage, 2)

    gross_pnl = round((sell_price - buy_price) * qty, 2)
    net_pnl = round(gross_pnl - total_friction, 2)

    return {
        "instrument_type": inst,
        "qty": qty,
        "buy_price": round(buy_price, 2),
        "sell_price": round(sell_price, 2),
        "buy_turnover": round(buy_turnover, 2),
        "sell_turnover": round(sell_turnover, 2),
        "total_turnover": round(total_turnover, 2),
        "gross_pnl": gross_pnl,
        "net_pnl": net_pnl,
        "total_friction": total_friction,
        "breakdown": {
            "slippage": total_slippage,
            "brokerage": brokerage,
            "stt": stt,
            "exchange_charges": exchange_charges,
            "sebi_charges": sebi_charges,
            "stamp_duty": stamp_duty,
            "gst": gst,
            "taxes_and_statutory": total_taxes_and_charges,
        },
    }
