"""
Fetch daily prices for the Stock radar (runs on GitHub every 15 minutes).

  python fetch_radar_data.py auto   only refresh markets that are open now (used by the schedule)
  python fetch_radar_data.py all    refresh NSE and US
  python fetch_radar_data.py nse    refresh NSE only
  python fetch_radar_data.py us     refresh US only

Writes data/nse_daily.csv, data/us_daily.csv and data/meta.json.
A market that isn't refreshed keeps its last published file (copied from SITE_URL).
Prices come from Yahoo Finance (may be delayed by a few minutes).
"""
import datetime as dt
import io
import json
import os
import sys
import time

import pandas as pd
import requests
import yfinance as yf

PERIOD = "3y"
OUT = "data"
SITE_URL = os.environ.get("SITE_URL", "").rstrip("/")
FILES = {"NSE": "nse_daily.csv", "US": "us_daily.csv"}
DEFAULT_US = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "JPM", "V",
    "MA", "UNH", "XOM", "LLY", "JNJ", "PG", "HD", "COST", "ABBV", "MRK",
    "PEP", "KO", "BAC", "WMT", "CRM", "AMD", "NFLX", "ADBE", "ORCL", "INTC",
]


NIFTY200_BUILTIN = [
    "360ONE", "ABB", "APLAPOLLO", "AUBANK", "ADANIENSOL", "ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER", "ATGL",
    "ABCAPITAL", "AMBUJACEM", "APARINDS", "APOLLOHOSP", "ASHOKLEY", "ASIANPAINT", "ASTRAL", "AUROPHARMA", "DMART", "AXISBANK",
    "BSE", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BAJAJHLDNG", "BANKBARODA", "BANKINDIA", "MAHABANK", "BDL", "BEL",
    "BHARATFORG", "BHEL", "BPCL", "BHARTIARTL", "GROWW", "BIOCON", "BLUESTARCO", "BOSCHLTD", "BRITANNIA", "CGPOWER",
    "CANBK", "CHOLAFIN", "CIPLA", "COALINDIA", "COCHINSHIP", "COFORGE", "COLPAL", "CONCOR", "CUMMINSIND", "DLF",
    "DABUR", "DIVISLAB", "DIXON", "DRREDDY", "EICHERMOT", "ETERNAL", "EXIDEIND", "NYKAA", "FEDERALBNK", "FORTIS",
    "GAIL", "GVT&D", "GMRAIRPORT", "GLENMARK", "GODFRYPHLP", "GODREJCP", "GODREJPROP", "GRASIM", "HCLTECH", "HDFCAMC",
    "HDFCBANK", "HDFCLIFE", "HAVELLS", "HEROMOTOCO", "HINDALCO", "HAL", "HINDCOPPER", "HINDPETRO", "HINDUNILVR", "HINDZINC",
    "POWERINDIA", "HYUNDAI", "ICICIBANK", "ICICIGI", "ICICIAMC", "IDFCFIRSTB", "ITC", "INDIANB", "INDHOTEL", "IOC",
    "IRCTC", "IRFC", "IREDA", "INDUSTOWER", "INDUSINDBK", "NAUKRI", "INFY", "INDIGO", "JSWENERGY", "JSWSTEEL",
    "JINDALSTEL", "JIOFIN", "JUBLFOOD", "KEI", "KALYANKJIL", "KOTAKBANK", "LTF", "LGEINDIA", "LICHSGFIN", "LTM",
    "LT", "LAURUSLABS", "LENSKART", "LICI", "LODHA", "LUPIN", "MRF", "M&MFIN", "M&M", "MANKIND",
    "MARICO", "MARUTI", "MFSL", "MAXHEALTH", "MAZDOCK", "MEESHO", "MOTILALOFS", "MPHASIS", "MCX", "MUTHOOTFIN",
    "NHPC", "NLCINDIA", "NMDC", "NTPC", "NATIONALUM", "NESTLEIND", "OBEROIRLTY", "ONGC", "OIL", "PAYTM",
    "OFSS", "POLICYBZR", "PIIND", "PAGEIND", "PATANJALI", "PERSISTENT", "PHOENIXLTD", "PIDILITIND", "POLYCAB", "PFC",
    "POWERGRID", "PREMIERENE", "PRESTIGE", "PNB", "RECLTD", "RADICO", "RVNL", "RELIANCE", "SBICARD", "SBILIFE",
    "SRF", "MOTHERSON", "SHRIRAMFIN", "ENRIN", "SIEMENS", "SOLARINDS", "SBIN", "SAIL", "SUNPHARMA", "SUPREMEIND",
    "SUZLON", "SWIGGY", "TVSMOTOR", "TATACAP", "TATACOMM", "TCS", "TATACONSUM", "TMCV", "TMPV", "TATAPOWER",
    "TATASTEEL", "TECHM", "TITAN", "TORNTPHARM", "TRENT", "TIINDIA", "UPL", "ULTRACEMCO", "UNIONBANK", "UNITDSPR",
    "VBL", "VAML", "VEDL", "VMM", "IDEA", "VOLTAS", "WAAREEENER", "WIPRO", "YESBANK", "ZYDUSLIFE",
]


def nse_symbols():
    """Use ind_nifty200list.csv if it's in the repo, else the built-in list."""
    if os.path.exists("ind_nifty200list.csv"):
        df = pd.read_csv("ind_nifty200list.csv")
        df.columns = [c.strip() for c in df.columns]
        return [str(s).strip() for s in df["Symbol"].dropna()]
    print("ind_nifty200list.csv not found, using the built-in Nifty 200 list")
    return NIFTY200_BUILTIN


def us_symbols():
    if os.path.exists("us_tickers.txt"):
        with open("us_tickers.txt") as f:
            syms = [l.strip().upper() for l in f if l.strip() and not l.strip().startswith("#")]
        if syms:
            return syms
    return DEFAULT_US


def market_open_now(market):
    """Rough check in UTC, with a little margin. Holidays are ignored."""
    now = dt.datetime.now(dt.timezone.utc)
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    if market == "NSE":   # 09:15-15:30 IST = 03:45-10:00 UTC
        return 3 * 60 + 30 <= minutes <= 10 * 60 + 30
    return 13 * 60 + 15 <= minutes <= 21 * 60 + 15   # US, covers summer and winter time


def download(symbols, suffix):
    frames, missing = [], []
    for start in range(0, len(symbols), 50):           # batches are gentler on Yahoo
        batch = symbols[start:start + 50]
        tickers = [s + suffix for s in batch]
        data = None
        for attempt in range(3):
            try:
                data = yf.download(tickers, period=PERIOD, interval="1d", group_by="ticker",
                                   auto_adjust=False, threads=True, progress=False)
                if data is not None and not data.empty:
                    break
            except Exception as e:
                print(f"  attempt {attempt + 1} failed: {e}")
            time.sleep(10 * (attempt + 1))
        if data is None or data.empty:
            missing += batch
            continue
        for sym, tk in zip(batch, tickers):
            try:
                d = data[tk] if len(tickers) > 1 else data
                if isinstance(d.columns, pd.MultiIndex):
                    d = d.droplevel(0, axis=1)
                d = d[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
            except (KeyError, ValueError):
                d = pd.DataFrame()
            if d.empty:
                missing.append(sym)
                continue
            d = d.reset_index()
            d = d.rename(columns={d.columns[0]: "Date"})
            d["Date"] = pd.to_datetime(d["Date"]).dt.strftime("%Y-%m-%d")
            d.insert(1, "Symbol", sym)
            frames.append(d[["Date", "Symbol", "Open", "High", "Low", "Close", "Volume"]])
        time.sleep(2)
    if missing:
        print(f"  no data for {len(missing)}: {', '.join(missing[:30])}")
    if not frames:
        return None, 0
    out = pd.concat(frames)
    out["Volume"] = out["Volume"].fillna(0).astype("int64")
    return out.round(4), len(frames)


def previous_meta():
    if not SITE_URL:
        return {}
    try:
        r = requests.get(f"{SITE_URL}/data/meta.json", params={"t": time.time()}, timeout=30)
        return r.json().get("markets", {}) if r.ok else {}
    except Exception:
        return {}


def copy_previous(market):
    """Keep the last published file for a market we're not refreshing."""
    if not SITE_URL:
        return False
    try:
        r = requests.get(f"{SITE_URL}/data/{FILES[market]}", params={"t": time.time()}, timeout=60)
        if r.ok and r.text.startswith("Date,"):
            with open(os.path.join(OUT, FILES[market]), "w") as f:
                f.write(r.text)
            return True
    except Exception as e:
        print(f"  couldn't copy previous {market} data: {e}")
    return False


def main():
    mode = (sys.argv[1] if len(sys.argv) > 1 else "all").lower()
    os.makedirs(OUT, exist_ok=True)
    prev = previous_meta()
    meta = {"generatedAt": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "markets": {}}
    for market, suffix, syms in (("NSE", ".NS", nse_symbols()), ("US", "", us_symbols())):
        want = mode in ("all", market.lower()) or (mode == "auto" and market_open_now(market))
        if mode == "auto" and market not in prev:
            want = True      # first run: fetch everything
        if want:
            print(f"{market}: downloading {len(syms)} tickers…")
            df, n = download(syms, suffix)
            if df is not None and n >= 0.5 * len(syms):
                df.to_csv(os.path.join(OUT, FILES[market]), index=False)
                meta["markets"][market] = {"file": FILES[market], "symbols": n,
                                           "updatedAt": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
                print(f"{market}: saved {len(df):,} rows for {n} symbols")
                continue
            print(f"{market}: download incomplete ({n} of {len(syms)}), keeping previous data")
        if market in prev and copy_previous(market):
            meta["markets"][market] = prev[market]
            print(f"{market}: kept data from {prev[market].get('updatedAt')}")
    with open(os.path.join(OUT, "meta.json"), "w") as f:
        json.dump(meta, f, indent=1)
    if not meta["markets"]:
        sys.exit("No market data available. Check the log above.")


if __name__ == "__main__":
    main()
