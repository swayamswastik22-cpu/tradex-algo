import React, { useEffect, useState, useMemo } from "react";
import { useTradingContext, Timeframe, getApiBaseUrl } from "../context/TradingContext";

export interface Candle {
  time: number;
  time_str: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export const InteractiveChart: React.FC = () => {
  const {
    activeSymbol,
    activeTimeframe,
    setActiveTimeframe,
    live_prices,
    getTickerMetric,
    tickCounter,
  } = useTradingContext();

  const [candles, setCandles] = useState<Candle[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [hoveredCandle, setHoveredCandle] = useState<Candle | null>(null);

  const TIMEFRAMES: Timeframe[] = ["1m", "3m", "5m", "15m", "1d"];

  useEffect(() => {
    let isCancelled = false;

    const fetchCandles = async () => {
      setLoading(true);
      try {
        const baseUrl = getApiBaseUrl();
        const url = `${baseUrl}/api/market/candles?symbol=${encodeURIComponent(
          activeSymbol
        )}&interval=${activeTimeframe}&range=${
          activeTimeframe === "1d" ? "1mo" : activeTimeframe === "15m" ? "5d" : "1d"
        }`;
        const res = await fetch(url);
        if (!res.ok) throw new Error("Candles fetch failed");
        const json = await res.json();
        if (!isCancelled && Array.isArray(json.candles)) {
          setCandles(json.candles);
        }
      } catch (err) {
        console.warn("[InteractiveChart] Candles load notice:", err);
      } finally {
        if (!isCancelled) setLoading(false);
      }
    };

    fetchCandles();
    return () => {
      isCancelled = true;
    };
  }, [activeSymbol, activeTimeframe]);

  const liveLtp = live_prices[activeSymbol];
  const chartCandles = useMemo(() => {
    if (!candles || candles.length === 0) return [];
    if (!liveLtp || liveLtp <= 0) return candles;

    const copy = [...candles];
    const last = { ...copy[copy.length - 1] };
    last.close = liveLtp;
    if (liveLtp > last.high) last.high = liveLtp;
    if (liveLtp < last.low) last.low = liveLtp;
    copy[copy.length - 1] = last;
    return copy;
  }, [candles, liveLtp, tickCounter]);

  const metric = getTickerMetric(activeSymbol);

  const width = 800;
  const height = 360;
  const padding = { top: 20, right: 60, bottom: 40, left: 10 };
  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;

  const minPrice = useMemo(() => {
    if (chartCandles.length === 0) return 0;
    return Math.min(...chartCandles.map((c) => c.low)) * 0.999;
  }, [chartCandles]);

  const maxPrice = useMemo(() => {
    if (chartCandles.length === 0) return 100;
    return Math.max(...chartCandles.map((c) => c.high)) * 1.001;
  }, [chartCandles]);

  const maxVolume = useMemo(() => {
    if (chartCandles.length === 0) return 1;
    return Math.max(...chartCandles.map((c) => c.volume)) || 1;
  }, [chartCandles]);

  const getY = (val: number) => {
    if (maxPrice === minPrice) return chartHeight / 2;
    return padding.top + chartHeight - ((val - minPrice) / (maxPrice - minPrice)) * chartHeight;
  };

  const candleWidth = chartCandles.length > 0 ? Math.max(2, (chartWidth / chartCandles.length) * 0.7) : 4;

  return (
    <div className="bg-neutral-900/90 border border-neutral-800 rounded-2xl p-5 shadow-2xl flex flex-col">
      <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between pb-4 border-b border-neutral-800/80 gap-3">
        <div>
          <div className="flex items-center gap-3">
            <h2 className="text-xl font-mono font-extrabold text-neutral-100 tracking-wider">
              {activeSymbol}
            </h2>
            <span className="text-xs px-2 py-0.5 rounded font-mono font-bold bg-neutral-800 text-neutral-300">
              NSE Live
            </span>
          </div>

          <div className="flex items-baseline gap-3 mt-1.5">
            <span className="text-2xl font-black font-mono tracking-tight text-neutral-100">
              ₹{(metric.ltp || liveLtp || 0).toLocaleString("en-IN", { minimumFractionDigits: 2 })}
            </span>
            <div
              className={`flex items-center gap-1 font-mono text-sm font-bold ${
                metric.change >= 0 ? "text-emerald-400" : "text-rose-400"
              }`}
            >
              <span>{metric.change >= 0 ? "+" : ""}₹{metric.change.toFixed(2)}</span>
              <span>({metric.change >= 0 ? "+" : ""}{metric.change_pct.toFixed(2)}%)</span>
            </div>
          </div>
        </div>

        <div className="flex items-center bg-neutral-950 p-1 rounded-xl border border-neutral-800">
          {TIMEFRAMES.map((tf) => (
            <button
              key={tf}
              onClick={() => setActiveTimeframe(tf)}
              className={`px-3 py-1 rounded-lg text-xs font-mono font-bold transition ${
                activeTimeframe === tf
                  ? "bg-emerald-500 text-neutral-950 shadow-md scale-105"
                  : "text-neutral-400 hover:text-neutral-200 hover:bg-neutral-800"
              }`}
            >
              {tf.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      <div className="h-6 flex items-center gap-4 text-[11px] font-mono text-neutral-400 mt-2">
        {hoveredCandle ? (
          <>
            <span>Time: <b className="text-neutral-200">{hoveredCandle.time_str}</b></span>
            <span>O: <b className="text-neutral-200">₹{hoveredCandle.open.toFixed(2)}</b></span>
            <span>H: <b className="text-emerald-400">₹{hoveredCandle.high.toFixed(2)}</b></span>
            <span>L: <b className="text-rose-400">₹{hoveredCandle.low.toFixed(2)}</b></span>
            <span>C: <b className="text-neutral-100">₹{hoveredCandle.close.toFixed(2)}</b></span>
            <span>Vol: <b className="text-cyan-400">{hoveredCandle.volume.toLocaleString()}</b></span>
          </>
        ) : (
          <span>Hover over candles to view precise OHLC bars</span>
        )}
      </div>

      <div className="relative w-full h-[360px] mt-2 overflow-hidden select-none">
        {loading ? (
          <div className="absolute inset-0 flex items-center justify-center bg-neutral-900/60 backdrop-blur-sm z-10">
            <span className="w-8 h-8 border-2 border-emerald-500 border-t-transparent rounded-full animate-spin" />
          </div>
        ) : chartCandles.length === 0 ? (
          <div className="absolute inset-0 flex items-center justify-center text-xs text-neutral-500 font-mono">
            No candle data available for this timeframe.
          </div>
        ) : (
          <svg
            viewBox={`0 0 ${width} ${height}`}
            className="w-full h-full"
            preserveAspectRatio="none"
          >
            {[0.2, 0.4, 0.6, 0.8].map((pct) => {
              const y = padding.top + chartHeight * pct;
              const priceLabel = maxPrice - (maxPrice - minPrice) * pct;
              return (
                <g key={pct}>
                  <line
                    x1={padding.left}
                    y1={y}
                    x2={width - padding.right}
                    y2={y}
                    stroke="#262626"
                    strokeDasharray="3 3"
                    strokeWidth="1"
                  />
                  <text
                    x={width - padding.right + 6}
                    y={y + 3}
                    fill="#737373"
                    fontSize="9"
                    fontFamily="monospace"
                  >
                    ₹{priceLabel.toFixed(1)}
                  </text>
                </g>
              );
            })}

            {chartCandles.map((c, i) => {
              const x = padding.left + (i / chartCandles.length) * chartWidth + candleWidth / 2;
              const vHeight = (c.volume / maxVolume) * (chartHeight * 0.25);
              const vY = padding.top + chartHeight - vHeight;
              const isUp = c.close >= c.open;
              return (
                <rect
                  key={`vol-${i}`}
                  x={x - candleWidth / 2}
                  y={vY}
                  width={candleWidth}
                  height={Math.max(1, vHeight)}
                  fill={isUp ? "#065f46" : "#881337"}
                  opacity="0.4"
                />
              );
            })}

            {chartCandles.map((c, i) => {
              const x = padding.left + (i / chartCandles.length) * chartWidth + candleWidth / 2;
              const isUp = c.close >= c.open;
              const yOpen = getY(c.open);
              const yClose = getY(c.close);
              const yHigh = getY(c.high);
              const yLow = getY(c.low);

              const bodyY = Math.min(yOpen, yClose);
              const bodyHeight = Math.max(1.5, Math.abs(yClose - yOpen));
              const color = isUp ? "#34d399" : "#fb7185";

              return (
                <g
                  key={`candle-${i}`}
                  onMouseEnter={() => setHoveredCandle(c)}
                  onMouseLeave={() => setHoveredCandle(null)}
                  className="cursor-crosshair"
                >
                  <line
                    x1={x}
                    y1={yHigh}
                    x2={x}
                    y2={yLow}
                    stroke={color}
                    strokeWidth="1.2"
                  />
                  <rect
                    x={x - candleWidth / 2}
                    y={bodyY}
                    width={candleWidth}
                    height={bodyHeight}
                    fill={color}
                  />
                </g>
              );
            })}

            {liveLtp > 0 && (
              <g>
                <line
                  x1={padding.left}
                  y1={getY(liveLtp)}
                  x2={width - padding.right}
                  y2={getY(liveLtp)}
                  stroke="#10b981"
                  strokeWidth="1.2"
                  strokeDasharray="4 2"
                />
                <rect
                  x={width - padding.right + 2}
                  y={getY(liveLtp) - 9}
                  width="52"
                  height="16"
                  rx="3"
                  fill="#065f46"
                />
                <text
                  x={width - padding.right + 6}
                  y={getY(liveLtp) + 3}
                  fill="#a7f3d0"
                  fontSize="9.5"
                  fontWeight="bold"
                  fontFamily="monospace"
                >
                  ₹{liveLtp.toFixed(1)}
                </text>
              </g>
            )}
          </svg>
        )}
      </div>
    </div>
  );
};
