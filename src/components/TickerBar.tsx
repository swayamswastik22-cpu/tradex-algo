import React from "react";
import { useTradingContext } from "../context/TradingContext";

const TICKER_SYMBOLS = [
  "NIFTY",
  "BANKNIFTY",
  "RELIANCE",
  "HDFCBANK",
  "TCS",
  "INFY",
  "SBIN",
  "ICICIBANK",
  "LT",
  "ITC",
];

export const TickerBar: React.FC = () => {
  const { getTickerMetric, setActiveSymbol, activeSymbol } = useTradingContext();

  return (
    <div className="w-full bg-neutral-950/90 border-y border-neutral-800/80 px-2 py-2 overflow-x-auto no-scrollbar flex items-center gap-3">
      <div className="flex items-center gap-1 px-2.5 py-1 rounded bg-neutral-900 border border-neutral-800 text-[11px] font-bold text-neutral-400 uppercase tracking-wider shrink-0">
        <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
        <span>NSE Ticker</span>
      </div>

      <div className="flex items-center gap-2.5 shrink-0">
        {TICKER_SYMBOLS.map((sym) => {
          const metric = getTickerMetric(sym);
          const isUp = metric.change >= 0;
          const isActive = activeSymbol === sym;

          return (
            <button
              key={sym}
              onClick={() => setActiveSymbol(sym)}
              className={`flex items-center gap-2.5 px-3 py-1.5 rounded-xl border text-xs font-mono transition group cursor-pointer ${
                isActive
                  ? "bg-neutral-800 border-emerald-500/80 shadow-md"
                  : "bg-neutral-900/60 border-neutral-800/70 hover:bg-neutral-850 hover:border-neutral-700"
              }`}
            >
              <span className="font-extrabold text-neutral-200 group-hover:text-emerald-300">
                {sym}
              </span>

              <span className="font-bold text-neutral-100">
                ₹{metric.ltp > 0 ? metric.ltp.toLocaleString("en-IN", { minimumFractionDigits: 2 }) : "—"}
              </span>

              <span
                className={`flex items-center font-bold text-[11px] px-1.5 py-0.5 rounded ${
                  isUp ? "text-emerald-400 bg-emerald-950/60" : "text-rose-400 bg-rose-950/60"
                }`}
              >
                {isUp ? "+" : ""}{metric.change.toFixed(2)} ({isUp ? "+" : ""}{metric.change_pct.toFixed(2)}%)
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );
};
