import React, {
  createContext,
  useContext,
  useState,
  useEffect,
  useRef,
  useCallback,
  ReactNode,
} from "react";

// ============================================================================
// Types & Interfaces
// ============================================================================

export type PriceDirection = "up" | "down" | "neutral";
export type Timeframe = "1m" | "3m" | "5m" | "15m" | "1d";

export interface MarketData {
  exchange: string;
  is_open: boolean;
  session: string;
  opening_range_window: string;
  flat_by_time: string;
}

export interface TickerMetric {
  symbol: string;
  name?: string;
  ltp: number;
  prev_close: number;
  change: number;
  change_pct: number;
  direction: PriceDirection;
}

export interface PortfolioSummary {
  initial_capital: number;
  current_portfolio_value: number;
  virtual_cash_balance: number;
  invested_capital: number;
  realized_pnl: number;
  unrealized_pnl: number;
  total_pnl: number;
  total_trades: number;
  open_trades_count: number;
  closed_trades_count: number;
  win_rate_pct: number;
}

export interface PaperPosition {
  trade_id: string;
  symbol: string;
  side: "BUY" | "SELL";
  quantity: number;
  entry_price: number;
  stop_loss: number;
  target: number;
  current_price: number;
  unrealized_pnl: number;
  pnl_pct: number;
  status: "OPEN" | "CLOSED" | "CANCELLED";
  entry_time: string;
}

export interface FeedStatus {
  connected: boolean;
  provider: string;
  mode: string;
  account_holder: string;
  ticks_streamed: number;
  last_tick_time: string;
}

export interface ScripSearchResult {
  token: string;
  symbol: string;
  name: string;
  exch_seg: string;
  instrumenttype: string;
  category: "EQUITY" | "FUTURES" | "CALL" | "PUT" | "OPTIONS";
  expiry?: string;
  strike?: number;
  lotsize: number;
  tick_size: number;
}

export interface TradingState {
  live_prices: Record<string, number>;
  previous_closes: Record<string, number>;
  ticker_data: Record<string, TickerMetric>;
  market_data: MarketData;
  portfolio: PortfolioSummary;
  positions: PaperPosition[];
  feed: FeedStatus;
  tickCounter: number;
  lastUpdated: string;
  priceDirections: Record<string, PriceDirection>;
  isLive: boolean;
  activeSymbol: string;
  activeTimeframe: Timeframe;
}

export interface TradingContextValue extends TradingState {
  setActiveSymbol: (sym: string) => void;
  setActiveTimeframe: (tf: Timeframe) => void;
  getPrice: (symbol: string) => number;
  getPriceDirection: (symbol: string) => PriceDirection;
  getTickerMetric: (symbol: string) => TickerMetric;
  refetchState: () => Promise<void>;
  searchScrips: (query: string, type?: string) => Promise<ScripSearchResult[]>;
}

export const DEFAULT_PREVIOUS_CLOSES: Record<string, number> = {
  NIFTY: 23440.8,
  BANKNIFTY: 55002.2,
  RELIANCE: 1300.0,
  HDFCBANK: 700.0,
  TCS: 2501.7,
  INFY: 1161.0,
  SBIN: 987.0,
  ICICIBANK: 1387.4,
  LT: 3939.9,
  ITC: 266.4,
};

const INITIAL_MARKET_DATA: MarketData = {
  exchange: "NSE / BSE India",
  is_open: true,
  session: "Regular (09:15-15:30 IST)",
  opening_range_window: "09:15 - 09:30 IST",
  flat_by_time: "15:20 IST",
};

const INITIAL_PORTFOLIO: PortfolioSummary = {
  initial_capital: 1000000.0,
  current_portfolio_value: 1000000.0,
  virtual_cash_balance: 1000000.0,
  invested_capital: 0,
  realized_pnl: 0,
  unrealized_pnl: 0,
  total_pnl: 0,
  total_trades: 0,
  open_trades_count: 0,
  closed_trades_count: 0,
  win_rate_pct: 0,
};

const INITIAL_FEED: FeedStatus = {
  connected: false,
  provider: "Angel One SmartWebSocketV2",
  mode: "CONNECTING",
  account_holder: "",
  ticks_streamed: 0,
  last_tick_time: "",
};

const TradingContext = createContext<TradingContextValue | null>(null);

export const getApiBaseUrl = (): string => {
  if (typeof import.meta !== "undefined" && (import.meta as any).env?.VITE_API_BASE_URL) {
    return (import.meta as any).env.VITE_API_BASE_URL;
  }
  if (typeof process !== "undefined" && process.env?.REACT_APP_API_BASE_URL) {
    return process.env.REACT_APP_API_BASE_URL;
  }
  if (typeof window !== "undefined" && window.location.port !== "7860") {
    return `${window.location.protocol}//${window.location.hostname}:7860`;
  }
  return "";
};

interface TradingProviderProps {
  children: ReactNode;
  pollIntervalMs?: number;
}

export const TradingProvider: React.FC<TradingProviderProps> = ({
  children,
  pollIntervalMs = 1000,
}) => {
  const [livePrices, setLivePrices] = useState<Record<string, number>>({});
  const [previousCloses, setPreviousCloses] = useState<Record<string, number>>(DEFAULT_PREVIOUS_CLOSES);
  const [tickerData, setTickerData] = useState<Record<string, TickerMetric>>({});
  const [marketData, setMarketData] = useState<MarketData>(INITIAL_MARKET_DATA);
  const [portfolio, setPortfolio] = useState<PortfolioSummary>(INITIAL_PORTFOLIO);
  const [positions, setPositions] = useState<PaperPosition[]>([]);
  const [feed, setFeed] = useState<FeedStatus>(INITIAL_FEED);
  const [tickCounter, setTickCounter] = useState<number>(0);
  const [lastUpdated, setLastUpdated] = useState<string>("");
  const [priceDirections, setPriceDirections] = useState<Record<string, PriceDirection>>({});
  const [isLive, setIsLive] = useState<boolean>(false);
  const [activeSymbol, setActiveSymbol] = useState<string>("NIFTY");
  const [activeTimeframe, setActiveTimeframe] = useState<Timeframe>("5m");

  const prevPricesRef = useRef<Record<string, number>>({});

  const refetchState = useCallback(async () => {
    try {
      const baseUrl = getApiBaseUrl();
      const res = await fetch(`${baseUrl}/api/state`, {
        headers: { Accept: "application/json" },
        cache: "no-store",
      });

      if (!res.ok) {
        throw new Error(`Failed to fetch /api/state: ${res.status}`);
      }

      const data = await res.json();

      const incomingPrices: Record<string, number> =
        data.live_prices || data.feed?.latest_prices || {};
      const incomingPrevCloses: Record<string, number> =
        data.previous_closes || DEFAULT_PREVIOUS_CLOSES;

      setPreviousCloses((prev) => ({ ...prev, ...incomingPrevCloses }));

      if (Object.keys(incomingPrices).length > 0) {
        const directions: Record<string, PriceDirection> = {};
        const computedTickers: Record<string, TickerMetric> = {};
        const prev = prevPricesRef.current;

        for (const [sym, newPrice] of Object.entries(incomingPrices)) {
          const oldPrice = prev[sym];
          let dir: PriceDirection = "neutral";
          if (oldPrice !== undefined) {
            if (newPrice > oldPrice) dir = "up";
            else if (newPrice < oldPrice) dir = "down";
          }
          directions[sym] = dir;

          const close = incomingPrevCloses[sym] || DEFAULT_PREVIOUS_CLOSES[sym] || newPrice;
          const diff = Number((newPrice - close).toFixed(2));
          const pct = close > 0 ? Number(((diff / close) * 100).toFixed(2)) : 0;

          computedTickers[sym] = {
            symbol: sym,
            ltp: newPrice,
            prev_close: close,
            change: diff,
            change_pct: pct,
            direction: diff > 0 ? "up" : diff < 0 ? "down" : "neutral",
          };
        }

        prevPricesRef.current = incomingPrices;
        setPriceDirections(directions);
        setLivePrices(incomingPrices);
        setTickerData(computedTickers);
      }

      const incomingMarket: MarketData = data.market_data || data.market;
      if (incomingMarket) {
        setMarketData(incomingMarket);
      }

      if (data.paper_oms?.portfolio) {
        setPortfolio(data.paper_oms.portfolio);
      }
      if (Array.isArray(data.paper_oms?.active_positions)) {
        setPositions(data.paper_oms.active_positions);
      }

      if (data.feed) {
        setFeed({
          connected: Boolean(data.feed.connected),
          provider: data.feed.provider || "Angel One SmartWebSocketV2",
          mode: data.feed.mode || "LIVE",
          account_holder: data.feed.account_holder || "",
          ticks_streamed: data.feed.ticks_streamed || 0,
          last_tick_time: data.feed.last_tick_time || "",
        });
      }

      setTickCounter((prev) => prev + 1);
      setLastUpdated(data.timestamp || new Date().toISOString());
      setIsLive(true);
    } catch (err) {
      console.warn("[TradingContext] Tick poll update notice:", err);
      setIsLive(false);
    }
  }, []);

  useEffect(() => {
    refetchState();
    const intervalId = setInterval(refetchState, pollIntervalMs);
    return () => clearInterval(intervalId);
  }, [refetchState, pollIntervalMs]);

  const getPrice = useCallback(
    (symbol: string): number => {
      const cleanSym = symbol.replace(".NS", "").replace(".BO", "").toUpperCase();
      return livePrices[cleanSym] ?? 0;
    },
    [livePrices]
  );

  const getPriceDirection = useCallback(
    (symbol: string): PriceDirection => {
      const cleanSym = symbol.replace(".NS", "").replace(".BO", "").toUpperCase();
      return priceDirections[cleanSym] ?? "neutral";
    },
    [priceDirections]
  );

  const getTickerMetric = useCallback(
    (symbol: string): TickerMetric => {
      const cleanSym = symbol.replace(".NS", "").replace(".BO", "").toUpperCase();
      if (tickerData[cleanSym]) {
        return tickerData[cleanSym];
      }
      const ltp = livePrices[cleanSym] ?? 0;
      const prevClose = previousCloses[cleanSym] ?? DEFAULT_PREVIOUS_CLOSES[cleanSym] ?? ltp;
      const diff = ltp > 0 && prevClose > 0 ? Number((ltp - prevClose).toFixed(2)) : 0;
      const pct = prevClose > 0 ? Number(((diff / prevClose) * 100).toFixed(2)) : 0;

      return {
        symbol: cleanSym,
        ltp,
        prev_close: prevClose,
        change: diff,
        change_pct: pct,
        direction: diff > 0 ? "up" : diff < 0 ? "down" : "neutral",
      };
    },
    [tickerData, livePrices, previousCloses]
  );

  const searchScrips = useCallback(async (query: string, type: string = "ALL"): Promise<ScripSearchResult[]> => {
    if (!query.trim()) return [];
    try {
      const baseUrl = getApiBaseUrl();
      const url = `${baseUrl}/api/market/scrip-search?q=${encodeURIComponent(query)}&type=${encodeURIComponent(type)}&limit=30`;
      const res = await fetch(url);
      if (!res.ok) return [];
      const json = await res.json();
      return json.results || [];
    } catch (e) {
      console.error("[searchScrips] error:", e);
      return [];
    }
  }, []);

  const value: TradingContextValue = {
    live_prices: livePrices,
    previous_closes: previousCloses,
    ticker_data: tickerData,
    market_data: marketData,
    portfolio,
    positions,
    feed,
    tickCounter,
    lastUpdated,
    priceDirections,
    isLive,
    activeSymbol,
    activeTimeframe,
    setActiveSymbol,
    setActiveTimeframe,
    getPrice,
    getPriceDirection,
    getTickerMetric,
    refetchState,
    searchScrips,
  };

  return <TradingContext.Provider value={value}>{children}</TradingContext.Provider>;
};

export const useTradingContext = (): TradingContextValue => {
  const context = useContext(TradingContext);
  if (!context) {
    throw new Error("useTradingContext must be used within a TradingProvider");
  }
  return context;
};

export const useLivePrice = (symbol: string) => {
  const { live_prices, priceDirections, tickCounter, isLive, getTickerMetric } = useTradingContext();
  const cleanSym = symbol.replace(".NS", "").replace(".BO", "").toUpperCase();
  const price = live_prices[cleanSym] ?? 0;
  const direction = priceDirections[cleanSym] ?? "neutral";
  const metric = getTickerMetric(cleanSym);

  return { price, direction, metric, tickCounter, isLive };
};
