import React from "react";
import { useTradingContext } from "../context/TradingContext";
import { UniversalSearchBar } from "./UniversalSearchBar";
import { TickerBar } from "./TickerBar";
import { InteractiveChart } from "./InteractiveChart";

const RADAR_EQUITIES = [
  { symbol: "RELIANCE", name: "Reliance Industries", sector: "Oil & Gas" },
  { symbol: "HDFCBANK", name: "HDFC Bank", sector: "Banking" },
  { symbol: "TCS", name: "Tata Consultancy", sector: "IT Services" },
  { symbol: "INFY", name: "Infosys", sector: "IT Services" },
  { symbol: "SBIN", name: "State Bank of India", sector: "Banking" },
  { symbol: "ICICIBANK", name: "ICICI Bank", sector: "Banking" },
  { symbol: "LT", name: "Larsen & Toubro", sector: "Infrastructure" },
  { symbol: "ITC", name: "ITC Ltd", sector: "FMCG" },
];

export const Dashboard: React.FC = () => {
  const {
    market_data,
    portfolio,
    positions,
    feed,
    tickCounter,
    lastUpdated,
    getTickerMetric,
    setActiveSymbol,
    activeSymbol,
    refetchState,
  } = useTradingContext();

  const niftyMetric = getTickerMetric("NIFTY");
  const bankNiftyMetric = getTickerMetric("BANKNIFTY");

  return (
    <div className="min-h-screen bg-neutral-950 text-neutral-100 font-sans flex flex-col">
      {/* 1. Top Navbar with Angel One-Modeled Universal Search Bar */}
      <nav className="bg-neutral-900/95 border-b border-neutral-800 px-4 md:px-8 py-3.5 flex flex-col md:flex-row items-center justify-between gap-4 sticky top-0 z-40 backdrop-blur-md">
        <div className="flex items-center gap-4 w-full md:w-auto justify-between md:justify-start">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-emerald-500/20 border border-emerald-500/40 flex items-center justify-center font-black text-emerald-400 font-mono text-base">
              TX
            </div>
            <div>
              <span className="text-base font-extrabold tracking-tight bg-gradient-to-r from-emerald-400 via-teal-300 to-cyan-400 bg-clip-text text-transparent">
                TRADEx-ALGO
              </span>
              <div className="text-[10px] text-neutral-400 font-mono uppercase tracking-wider">
                NSE Algorithmic Terminal
              </div>
            </div>
          </div>
          <span className="md:hidden px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
            Live
          </span>
        </div>

        {/* Feature 1: Universal Symbol Search Bar */}
        <div className="w-full md:max-w-xl">
          <UniversalSearchBar />
        </div>

        {/* WebSocket Feed Status Pill */}
        <div className="hidden lg:flex items-center gap-3 bg-neutral-950/80 border border-neutral-800 rounded-xl px-3.5 py-1.5">
          <div className="relative flex items-center justify-center">
            <span
              className={`h-2.5 w-2.5 rounded-full ${
                feed.connected ? "bg-emerald-500 animate-ping" : "bg-amber-500"
              }`}
            />
            <span
              className={`absolute h-2 w-2 rounded-full ${
                feed.connected ? "bg-emerald-400" : "bg-amber-400"
              }`}
            />
          </div>
          <div className="text-xs">
            <div className="font-semibold text-neutral-200">
              {feed.provider}
            </div>
            <div className="text-[11px] text-neutral-400 flex items-center gap-1.5">
              <span>Ticks: <b className="text-neutral-200 font-mono">{feed.ticks_streamed.toLocaleString()}</b></span>
              <span>•</span>
              <span>Render: <b className="text-cyan-400 font-mono">#{tickCounter}</b></span>
            </div>
          </div>
          <button
            onClick={() => refetchState()}
            className="ml-1 p-1.5 text-neutral-400 hover:text-neutral-200 bg-neutral-900 rounded hover:bg-neutral-800 transition"
            title="Refresh state"
          >
            ↻
          </button>
        </div>
      </nav>

      {/* Feature 3: Dynamic Ticker Bar across all symbols */}
      <TickerBar />

      {/* Main Content Area */}
      <main className="p-4 md:p-8 space-y-6 flex-1 max-w-[1600px] w-full mx-auto">
        {/* Index Headers with Dynamic Price Changes */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {/* Nifty 50 Card */}
          <div
            onClick={() => setActiveSymbol("NIFTY")}
            className={`p-4 rounded-2xl border transition cursor-pointer ${
              activeSymbol === "NIFTY"
                ? "bg-neutral-900 border-emerald-500/80 shadow-lg"
                : "bg-neutral-900/60 border-neutral-800 hover:border-neutral-700"
            }`}
          >
            <div className="flex items-center justify-between">
              <span className="text-xs font-mono font-bold text-neutral-300">NIFTY 50</span>
              <span className="text-[10px] px-1.5 py-0.5 rounded font-mono font-bold bg-cyan-950 text-cyan-300 border border-cyan-800">
                BENCHMARK
              </span>
            </div>
            <div className="text-2xl font-black font-mono text-neutral-100 mt-2">
              ₹{niftyMetric.ltp.toLocaleString("en-IN", { minimumFractionDigits: 2 })}
            </div>
            <div
              className={`text-xs font-mono font-bold mt-1 flex items-center gap-1.5 ${
                niftyMetric.change >= 0 ? "text-emerald-400" : "text-rose-400"
              }`}
            >
              <span>{niftyMetric.change >= 0 ? "+" : ""}₹{niftyMetric.change.toFixed(2)}</span>
              <span>({niftyMetric.change >= 0 ? "+" : ""}{niftyMetric.change_pct.toFixed(2)}%)</span>
            </div>
          </div>

          {/* Bank Nifty Card */}
          <div
            onClick={() => setActiveSymbol("BANKNIFTY")}
            className={`p-4 rounded-2xl border transition cursor-pointer ${
              activeSymbol === "BANKNIFTY"
                ? "bg-neutral-900 border-emerald-500/80 shadow-lg"
                : "bg-neutral-900/60 border-neutral-800 hover:border-neutral-700"
            }`}
          >
            <div className="flex items-center justify-between">
              <span className="text-xs font-mono font-bold text-neutral-300">BANK NIFTY</span>
              <span className="text-[10px] px-1.5 py-0.5 rounded font-mono font-bold bg-purple-950 text-purple-300 border border-purple-800">
                BANKING
              </span>
            </div>
            <div className="text-2xl font-black font-mono text-neutral-100 mt-2">
              ₹{bankNiftyMetric.ltp.toLocaleString("en-IN", { minimumFractionDigits: 2 })}
            </div>
            <div
              className={`text-xs font-mono font-bold mt-1 flex items-center gap-1.5 ${
                bankNiftyMetric.change >= 0 ? "text-emerald-400" : "text-rose-400"
              }`}
            >
              <span>{bankNiftyMetric.change >= 0 ? "+" : ""}₹{bankNiftyMetric.change.toFixed(2)}</span>
              <span>({bankNiftyMetric.change >= 0 ? "+" : ""}{bankNiftyMetric.change_pct.toFixed(2)}%)</span>
            </div>
          </div>

          {/* Market Session Banner */}
          <div className="p-4 rounded-2xl bg-neutral-900/60 border border-neutral-800">
            <div className="text-xs text-neutral-400">Market Status</div>
            <div className="text-sm font-bold text-neutral-200 mt-2 flex items-center gap-2">
              <span className={`w-2.5 h-2.5 rounded-full ${market_data.is_open ? "bg-emerald-400 animate-pulse" : "bg-rose-400"}`} />
              {market_data.session}
            </div>
            <div className="text-[11px] text-neutral-400 mt-1">
              ORB: <b className="text-cyan-400 font-mono">{market_data.opening_range_window}</b> • Flat-by: <b className="text-amber-400 font-mono">{market_data.flat_by_time}</b>
            </div>
          </div>

          {/* Paper OMS Quick Value */}
          <div className="p-4 rounded-2xl bg-neutral-900/60 border border-neutral-800">
            <div className="text-xs text-neutral-400">Virtual Portfolio</div>
            <div className="text-2xl font-black font-mono text-neutral-100 mt-2">
              ₹{portfolio.current_portfolio_value.toLocaleString("en-IN", { minimumFractionDigits: 2 })}
            </div>
            <div
              className={`text-xs font-mono font-bold mt-1 ${
                portfolio.total_pnl >= 0 ? "text-emerald-400" : "text-rose-400"
              }`}
            >
              P&L: {portfolio.total_pnl >= 0 ? "+" : ""}₹{portfolio.total_pnl.toFixed(2)} (Virtual)
            </div>
          </div>
        </div>

        {/* Feature 2: Interactive Chart with Dynamic Timeframes (1m, 3m, 5m, 15m, 1D) */}
        <section>
          <InteractiveChart />
        </section>

        {/* Equities Radar & Active Paper OMS Positions */}
        <section className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Equities Radar Table with dynamic price change percentages */}
          <div className="lg:col-span-2 bg-neutral-900/60 border border-neutral-800 rounded-2xl p-5">
            <div className="flex items-center justify-between mb-4">
              <div>
                <h3 className="text-sm font-extrabold uppercase tracking-wider text-neutral-200">
                  Equities Radar
                </h3>
                <div className="text-xs text-neutral-400">
                  Click any stock to switch active chart & feed
                </div>
              </div>
              <span className="text-xs text-emerald-400 bg-emerald-950/60 px-2 py-0.5 rounded border border-emerald-800/40 font-mono">
                Real-time WebSocket Feed
              </span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs font-mono">
                <thead>
                  <tr className="border-b border-neutral-800 text-neutral-400">
                    <th className="pb-2.5">Symbol</th>
                    <th className="pb-2.5">Sector</th>
                    <th className="pb-2.5">Prev Close</th>
                    <th className="pb-2.5">Live LTP</th>
                    <th className="pb-2.5">Change (₹)</th>
                    <th className="pb-2.5 text-right">%Chg</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-neutral-800/40">
                  {RADAR_EQUITIES.map((eq) => {
                    const metric = getTickerMetric(eq.symbol);
                    const isUp = metric.change >= 0;
                    const isSelected = activeSymbol === eq.symbol;

                    return (
                      <tr
                        key={eq.symbol}
                        onClick={() => setActiveSymbol(eq.symbol)}
                        className={`hover:bg-neutral-850 cursor-pointer transition ${
                          isSelected ? "bg-neutral-800/60" : ""
                        }`}
                      >
                        <td className="py-3 font-bold text-neutral-100 flex items-center gap-2">
                          <span>{eq.symbol}</span>
                          {isSelected && (
                            <span className="text-[9px] px-1 py-0.2 rounded bg-emerald-500 text-neutral-950 font-bold">
                              ACTIVE
                            </span>
                          )}
                        </td>
                        <td className="py-3 text-neutral-400">{eq.sector}</td>
                        <td className="py-3 text-neutral-400">₹{metric.prev_close.toFixed(2)}</td>
                        <td className="py-3 font-bold text-neutral-100">
                          ₹{metric.ltp > 0 ? metric.ltp.toFixed(2) : "—"}
                        </td>
                        <td
                          className={`py-3 font-bold ${
                            isUp ? "text-emerald-400" : "text-rose-400"
                          }`}
                        >
                          {isUp ? "+" : ""}₹{metric.change.toFixed(2)}
                        </td>
                        <td
                          className={`py-3 text-right font-bold ${
                            isUp ? "text-emerald-400" : "text-rose-400"
                          }`}
                        >
                          <span
                            className={`px-2 py-0.5 rounded text-[11px] ${
                              isUp ? "bg-emerald-950/80" : "bg-rose-950/80"
                            }`}
                          >
                            {isUp ? "+" : ""}{metric.change_pct.toFixed(2)}%
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {/* Paper OMS Positions Table */}
          <div className="bg-neutral-900/60 border border-neutral-800 rounded-2xl p-5 flex flex-col">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-extrabold uppercase tracking-wider text-neutral-200">
                Paper Positions ({positions.length})
              </h3>
              <span className="text-[10px] px-2 py-0.5 rounded bg-emerald-950 text-emerald-400 border border-emerald-800">
                100% Virtual
              </span>
            </div>

            {positions.length === 0 ? (
              <div className="flex-1 flex flex-col items-center justify-center p-8 text-center text-xs text-neutral-500 border border-dashed border-neutral-800 rounded-xl">
                <p>No active paper positions open.</p>
                <p className="mt-1 text-[11px] text-neutral-600">
                  ORB Strategy scanning 09:15-09:30 range breakouts.
                </p>
              </div>
            ) : (
              <div className="space-y-3 overflow-y-auto">
                {positions.map((pos) => (
                  <div
                    key={pos.trade_id}
                    className="p-3 bg-neutral-900 border border-neutral-800 rounded-xl text-xs font-mono"
                  >
                    <div className="flex justify-between items-center mb-1">
                      <span className="font-bold text-neutral-100">{pos.symbol}</span>
                      <span
                        className={`px-1.5 py-0.2 rounded text-[10px] font-bold ${
                          pos.side === "BUY" ? "bg-emerald-950 text-emerald-400" : "bg-rose-950 text-rose-400"
                        }`}
                      >
                        {pos.side} {pos.quantity} Qty
                      </span>
                    </div>
                    <div className="flex justify-between text-[11px] text-neutral-400">
                      <span>Entry: ₹{pos.entry_price}</span>
                      <span>LTP: ₹{pos.current_price}</span>
                    </div>
                    <div className="flex justify-between text-[11px] mt-1 pt-1 border-t border-neutral-800">
                      <span>SL: ₹{pos.stop_loss}</span>
                      <span
                        className={`font-bold ${
                          pos.unrealized_pnl >= 0 ? "text-emerald-400" : "text-rose-400"
                        }`}
                      >
                        P&L: {pos.unrealized_pnl >= 0 ? "+" : ""}₹{pos.unrealized_pnl.toFixed(2)}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </section>
      </main>
    </div>
  );
};
