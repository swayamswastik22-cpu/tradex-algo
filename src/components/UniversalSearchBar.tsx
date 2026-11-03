import React, { useState, useEffect, useRef } from "react";
import { useTradingContext, ScripSearchResult, getApiBaseUrl } from "../context/TradingContext";

export const UniversalSearchBar: React.FC = () => {
  const { setActiveSymbol, searchScrips } = useTradingContext();
  const [query, setQuery] = useState("");
  const [activeTab, setActiveTab] = useState<"ALL" | "EQUITY" | "FUTURES" | "OPTIONS">("ALL");
  const [results, setResults] = useState<ScripSearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!query.trim()) {
      setResults([]);
      setLoading(false);
      return;
    }

    const timer = setTimeout(async () => {
      setLoading(true);
      const res = await searchScrips(query, activeTab);
      setResults(res);
      setLoading(false);
      setIsOpen(true);
    }, 200);

    return () => clearTimeout(timer);
  }, [query, activeTab, searchScrips]);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const handleSelect = async (item: ScripSearchResult) => {
    const cleanSym = item.symbol.endsWith("-EQ") ? item.symbol.replace("-EQ", "") : item.symbol;
    setActiveSymbol(cleanSym);
    setIsOpen(false);
    setQuery("");

    // Instantly switch and subscribe the active chart & websocket feed
    try {
      const baseUrl = getApiBaseUrl();
      await fetch(
        `${baseUrl}/api/feed/subscribe?symbol=${encodeURIComponent(cleanSym)}&token=${encodeURIComponent(
          item.token
        )}&exch_seg=${encodeURIComponent(item.exch_seg || "NSE")}`,
        { method: "POST" }
      );
    } catch (e) {
      console.warn("[UniversalSearchBar] dynamic feed subscribe notice:", e);
    }
  };

  const getBadgeStyle = (category: string) => {
    switch (category) {
      case "CALL":
        return "bg-emerald-950 text-emerald-400 border-emerald-700/60";
      case "PUT":
        return "bg-rose-950 text-rose-400 border-rose-700/60";
      case "FUTURES":
        return "bg-purple-950 text-purple-400 border-purple-700/60";
      case "EQUITY":
      default:
        return "bg-blue-950 text-blue-400 border-blue-700/60";
    }
  };

  return (
    <div ref={containerRef} className="relative w-full max-w-xl">
      <div className="relative flex items-center bg-neutral-900 border border-neutral-700 focus-within:border-emerald-500 rounded-xl transition shadow-lg overflow-hidden">
        <span className="pl-3.5 pr-2 text-neutral-400">
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
        </span>

        <input
          type="text"
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            if (!isOpen && e.target.value.trim()) setIsOpen(true);
          }}
          onFocus={() => {
            if (results.length > 0) setIsOpen(true);
          }}
          placeholder="Search Stocks, Futures (FUT), Options (24000 CE/PE)..."
          className="w-full py-2.5 bg-transparent text-sm text-neutral-100 placeholder-neutral-500 focus:outline-none"
        />

        {loading && (
          <div className="pr-3 flex items-center">
            <span className="w-3.5 h-3.5 border-2 border-emerald-500 border-t-transparent rounded-full animate-spin" />
          </div>
        )}

        {query && !loading && (
          <button
            onClick={() => {
              setQuery("");
              setResults([]);
              setIsOpen(false);
            }}
            className="pr-3 text-neutral-400 hover:text-neutral-200 text-xs transition"
          >
            ✕
          </button>
        )}
      </div>

      {isOpen && (
        <div className="absolute left-0 right-0 mt-2 bg-neutral-900/98 backdrop-blur-md border border-neutral-700 rounded-xl shadow-2xl z-50 overflow-hidden max-h-96 flex flex-col">
          <div className="flex items-center gap-1 p-2 border-b border-neutral-800 bg-neutral-950/70 text-xs">
            {(["ALL", "EQUITY", "FUTURES", "OPTIONS"] as const).map((tab) => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                className={`px-3 py-1 rounded-lg font-medium transition ${
                  activeTab === tab
                    ? "bg-emerald-500/20 text-emerald-400 border border-emerald-500/30"
                    : "text-neutral-400 hover:text-neutral-200 hover:bg-neutral-800"
                }`}
              >
                {tab}
              </button>
            ))}
          </div>

          <div className="overflow-y-auto divide-y divide-neutral-800/60">
            {results.length === 0 ? (
              <div className="p-6 text-center text-xs text-neutral-500">
                {query ? "No instruments matching query." : "Type a symbol to search."}
              </div>
            ) : (
              results.map((item) => (
                <div
                  key={`${item.token}-${item.symbol}`}
                  onClick={() => handleSelect(item)}
                  className="p-3 hover:bg-neutral-800/80 cursor-pointer transition flex items-center justify-between group"
                >
                  <div className="flex items-center gap-3">
                    <span
                      className={`text-[10px] font-bold px-2 py-0.5 rounded border uppercase tracking-wider font-mono ${getBadgeStyle(
                        item.category
                      )}`}
                    >
                      {item.category === "CALL" ? "CE" : item.category === "PUT" ? "PE" : item.category}
                    </span>

                    <div>
                      <div className="text-sm font-semibold text-neutral-100 group-hover:text-emerald-400 font-mono transition">
                        {item.symbol}
                      </div>
                      <div className="text-[11px] text-neutral-400 flex items-center gap-2">
                        <span>{item.name}</span>
                        {item.expiry && <span>• Exp: {item.expiry}</span>}
                        {item.strike && <span>• Strike: ₹{item.strike}</span>}
                      </div>
                    </div>
                  </div>

                  <div className="text-right">
                    <span className="text-[11px] font-mono text-neutral-400">
                      Lot: {item.lotsize}
                    </span>
                    <div className="text-[10px] uppercase text-neutral-500 font-mono">
                      {item.exch_seg}
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
};
