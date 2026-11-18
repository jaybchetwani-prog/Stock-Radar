"""
Fetch daily prices for the Stock radar (runs on GitHub every 15 minutes).

  python fetch_radar_data.py auto   refresh markets that are open now (used by the schedule)
  python fetch_radar_data.py all    full refresh of NSE and US
  python fetch_radar_data.py nse    full refresh of NSE only
  python fetch_radar_data.py us     full refresh of US only

Writes into data/:
  nse_history.csv, us_history.csv   3 years of daily candles (rebuilt once a day)
  nse_recent.csv,  us_recent.csv    last 5 days (refreshed every run, small and fast)
  ind_nifty200list.csv, ind_nifty500list.csv   index lists for the page
  meta.json                          what was updated and when

NSE stocks scanned = Nifty 200 list + Nifty 500 list + extra_tickers.txt (one NSE symbol per line,
for portfolio stocks outside the Nifty 500). Prices come from Yahoo Finance.
"""
import datetime as dt
import io
import json
import os
import shutil
import sys
import time

import pandas as pd
import requests
import yfinance as yf

PERIOD = "3y"
OUT = "data"
SITE_URL = os.environ.get("SITE_URL", "").rstrip("/")
NSE_LIST_URL = "https://archives.nseindia.com/content/indices/{}"
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


def read_list(name):
    """Read an NSE index list from the repo; try NSE's site if it's missing. Copies it to data/."""
    if not os.path.exists(name):
        try:
            r = requests.get(NSE_LIST_URL.format(name), headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            if r.ok and "Symbol" in r.text[:300]:
                with open(name, "w") as f:
                    f.write(r.text)
                print(f"Downloaded {name} from NSE")
        except Exception as e:
            print(f"Couldn't download {name}: {e}")
    if not os.path.exists(name):
        return []
    shutil.copy(name, os.path.join(OUT, name))
    df = pd.read_csv(name)
    df.columns = [c.strip() for c in df.columns]
    return [str(x).strip() for x in df["Symbol"].dropna()]


def read_lines(name):
    if not os.path.exists(name):
        return []
    with open(name) as f:
        return [l.strip().upper() for l in f if l.strip() and not l.strip().startswith("#")]


def nse_symbols():
    syms = read_list("ind_nifty200list.csv")
    n500 = read_list("ind_nifty500list.csv")
    print(f"Nifty 200 list: {len(syms) or 'built-in'}, Nifty 500 list: {len(n500) or 'not found'}")
    if not syms:
        syms = list(NIFTY200_BUILTIN)
    extra = read_lines("extra_tickers.txt")
    seen, out = set(), []
    for x in syms + n500 + extra:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def us_symbols():
    return read_lines("us_tickers.txt") or DEFAULT_US


def market_open_now(market):
    """Rough check in UTC, with a little margin. Holidays are ignored."""
    now = dt.datetime.now(dt.timezone.utc)
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    if market == "NSE":   # 09:15-15:30 IST = 03:45-10:00 UTC
        return 3 * 60 + 30 <= minutes <= 10 * 60 + 30
    return 13 * 60 + 15 <= minutes <= 21 * 60 + 15


def download(symbols, suffix, period):
    frames, missing = [], []
    for start in range(0, len(symbols), 50):
        batch = symbols[start:start + 50]
        tickers = [s + suffix for s in batch]
        data = None
        for attempt in range(3):
            try:
                data = yf.download(tickers, period=period, interval="1d", group_by="ticker",
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
        print(f"  no data for {len(missing)}: {', '.join(missing[:40])}")
    if not frames:
        return None, 0
    out = pd.concat(frames)
    out["Volume"] = out["Volume"].fillna(0).astype("int64")
    return out.round(2), len(frames)


def previous_meta():
    if not SITE_URL:
        return {}
    try:
        r = requests.get(f"{SITE_URL}/data/meta.json", params={"t": time.time()}, timeout=30)
        return r.json().get("markets", {}) if r.ok else {}
    except Exception:
        return {}


def copy_previous(name):
    if not SITE_URL or not name:
        return False
    try:
        r = requests.get(f"{SITE_URL}/data/{name}", params={"t": time.time()}, timeout=120)
        if r.ok and r.text.startswith("Date,"):
            with open(os.path.join(OUT, name), "w") as f:
                f.write(r.text)
            return True
    except Exception as e:
        print(f"  couldn't copy previous {name}: {e}")
    return False


def now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def main():
    mode = (sys.argv[1] if len(sys.argv) > 1 else "all").lower()
    os.makedirs(OUT, exist_ok=True)
    prev = previous_meta()
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    meta = {"generatedAt": now_iso(), "markets": {}}
    for market, key, suffix, syms in (("NSE", "nse", ".NS", nse_symbols()), ("US", "us", "", us_symbols())):
        pm = prev.get(market, {})
        hist, recent = f"{key}_history.csv", f"{key}_recent.csv"
        refresh = mode in ("all", key) or (mode == "auto" and (market_open_now(market) or not pm))
        if not refresh:
            if copy_previous(pm.get("history") or pm.get("file")):
                copy_previous(pm.get("recent"))
                meta["markets"][market] = pm
                print(f"{market}: market closed, kept data from {pm.get('updatedAt')}")
            continue
        full = (mode != "auto" or pm.get("historyDay") != today or pm.get("requested") != len(syms)
                or not copy_previous(pm.get("history")))
        if not full:
            print(f"{market}: quick refresh of the last 5 days for {len(syms)} tickers…")
            df, n = download(syms, suffix, "5d")
            if df is not None and n >= 0.5 * len(syms):
                df.to_csv(os.path.join(OUT, recent), index=False)
                meta["markets"][market] = dict(pm, recent=recent, updatedAt=now_iso())
                print(f"{market}: recent prices saved for {n} symbols")
                continue
            print(f"{market}: quick refresh failed, trying a full download")
        print(f"{market}: full download of {len(syms)} tickers ({PERIOD})…")
        df, n = download(syms, suffix, PERIOD)
        if df is not None and n >= 0.5 * len(syms):
            df.to_csv(os.path.join(OUT, hist), index=False)
            with open(os.path.join(OUT, recent), "w") as f:
                f.write("Date,Symbol,Open,High,Low,Close,Volume\n")
            stamp = now_iso()
            meta["markets"][market] = {"history": hist, "historyAt": stamp, "historyDay": today,
                                       "recent": recent, "updatedAt": stamp, "symbols": n, "requested": len(syms)}
            print(f"{market}: saved {len(df):,} rows for {n} symbols")
        elif pm and copy_previous(pm.get("history") or pm.get("file")):
            copy_previous(pm.get("recent"))
            meta["markets"][market] = pm
            print(f"{market}: download incomplete ({n} of {len(syms)}), kept previous data")
        else:
            print(f"{market}: no data available")
    with open(os.path.join(OUT, "meta.json"), "w") as f:
        json.dump(meta, f, indent=1)
    if not meta["markets"]:
        sys.exit("No market data available. Check the log above.")


if __name__ == "__main__":
    main()
