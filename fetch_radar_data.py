"""
Fetch daily prices for the Chartviewer (runs on GitHub every 15 minutes).

  python fetch_radar_data.py auto   refresh markets that are open now (used by the schedule)
  python fetch_radar_data.py all    full refresh of NSE and US
  python fetch_radar_data.py nse    full refresh of NSE only
  python fetch_radar_data.py us     full refresh of US only

Writes into data/:
  nse_history.csv, us_history.csv   3 years of daily candles (rebuilt once a day)
  nse_recent.csv,  us_recent.csv    last 5 days (refreshed every run, small and fast)
  ind_nifty200list.csv, ind_nifty500list.csv   index lists for the page
  meta.json                          what was updated and when

NSE symbols scanned = Nifty 200 list + Nifty 500 list + the ETFs and indices below + extra_tickers.txt
(one NSE symbol per line, for portfolio stocks or ETFs not covered). Prices come from Yahoo Finance.
Also writes data/nse_groups.json (ETF and index names) for the page.
"""
import base64
import datetime as dt
import gzip
import io
import json
import re
import zipfile
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
# (symbol shown in the radar, Yahoo Finance ticker, name, TradingView symbol)
ETFS = [
    ("NIFTYBEES", "NIFTYBEES.NS", "Nippon India ETF Nifty 50 BeES", "NSE:NIFTYBEES"),
    ("BANKBEES", "BANKBEES.NS", "Nippon India ETF Nifty Bank BeES", "NSE:BANKBEES"),
    ("JUNIORBEES", "JUNIORBEES.NS", "Nippon India ETF Nifty Next 50 Junior BeES", "NSE:JUNIORBEES"),
    ("MID150BEES", "MID150BEES.NS", "Nippon India ETF Nifty Midcap 150", "NSE:MID150BEES"),
    ("ITBEES", "ITBEES.NS", "Nippon India ETF Nifty IT", "NSE:ITBEES"),
    ("PSUBNKBEES", "PSUBNKBEES.NS", "Nippon India ETF Nifty PSU Bank BeES", "NSE:PSUBNKBEES"),
    ("PHARMABEES", "PHARMABEES.NS", "Nippon India ETF Nifty Pharma", "NSE:PHARMABEES"),
    ("AUTOBEES", "AUTOBEES.NS", "Nippon India ETF Nifty Auto", "NSE:AUTOBEES"),
    ("INFRABEES", "INFRABEES.NS", "Nippon India ETF Nifty Infrastructure BeES", "NSE:INFRABEES"),
    ("CONSUMBEES", "CONSUMBEES.NS", "Nippon India ETF Nifty India Consumption", "NSE:CONSUMBEES"),
    ("DIVOPPBEES", "DIVOPPBEES.NS", "Nippon India ETF Nifty Dividend Opportunities 50", "NSE:DIVOPPBEES"),
    ("SHARIABEES", "SHARIABEES.NS", "Nippon India ETF Nifty50 Shariah BeES", "NSE:SHARIABEES"),
    ("HNGSNGBEES", "HNGSNGBEES.NS", "Nippon India ETF Hang Seng BeES", "NSE:HNGSNGBEES"),
    ("GOLDBEES", "GOLDBEES.NS", "Nippon India ETF Gold BeES", "NSE:GOLDBEES"),
    ("SILVERBEES", "SILVERBEES.NS", "Nippon India Silver ETF", "NSE:SILVERBEES"),
    ("LIQUIDBEES", "LIQUIDBEES.NS", "Nippon India ETF Nifty 1D Rate Liquid BeES", "NSE:LIQUIDBEES"),
    ("CPSEETF", "CPSEETF.NS", "CPSE ETF", "NSE:CPSEETF"),
    ("MON100", "MON100.NS", "Motilal Oswal Nasdaq 100 ETF", "NSE:MON100"),
    ("MAFANG", "MAFANG.NS", "Mirae Asset NYSE FANG+ ETF", "NSE:MAFANG"),
    ("MOM100", "MOM100.NS", "Motilal Oswal Nifty Midcap 100 ETF", "NSE:MOM100"),
    ("SETFNIF50", "SETFNIF50.NS", "SBI Nifty 50 ETF", "NSE:SETFNIF50"),
    ("ICICIB22", "ICICIB22.NS", "Bharat 22 ETF", "NSE:ICICIB22"),
    ("HDFCGOLD", "HDFCGOLD.NS", "HDFC Gold ETF", "NSE:HDFCGOLD"),
    ("HDFCSILVER", "HDFCSILVER.NS", "HDFC Silver ETF", "NSE:HDFCSILVER"),
    ("GOLDIETF", "GOLDIETF.NS", "ICICI Prudential Gold ETF", "NSE:GOLDIETF"),
    ("SILVERIETF", "SILVERIETF.NS", "ICICI Prudential Silver ETF", "NSE:SILVERIETF"),
    ("LIQUIDIETF", "LIQUIDIETF.NS", "ICICI Prudential Nifty 1D Rate Liquid ETF", "NSE:LIQUIDIETF"),
]
INDICES = [
    ("NIFTY50", "^NSEI", "Nifty 50", "NSE:NIFTY"),
    ("BANKNIFTY", "^NSEBANK", "Nifty Bank", "NSE:BANKNIFTY"),
    ("SENSEX", "^BSESN", "BSE Sensex", "BSE:SENSEX"),
    ("NIFTYNEXT50", "^NSMIDCP", "Nifty Next 50", "NSE:NIFTYJR"),
    ("NIFTY100", "^CNX100", "Nifty 100", "NSE:CNX100"),
    ("NIFTY200", "^CNX200", "Nifty 200", "NSE:CNX200"),
    ("NIFTY500", "^CRSLDX", "Nifty 500", "NSE:CNX500"),
    ("NIFTYMIDCAP50", "^NSEMDCP50", "Nifty Midcap 50", "NSE:NIFTYMIDCAP50"),
    ("NIFTYIT", "^CNXIT", "Nifty IT", "NSE:CNXIT"),
    ("NIFTYPHARMA", "^CNXPHARMA", "Nifty Pharma", "NSE:CNXPHARMA"),
    ("NIFTYAUTO", "^CNXAUTO", "Nifty Auto", "NSE:CNXAUTO"),
    ("NIFTYFMCG", "^CNXFMCG", "Nifty FMCG", "NSE:CNXFMCG"),
    ("NIFTYMETAL", "^CNXMETAL", "Nifty Metal", "NSE:CNXMETAL"),
    ("NIFTYREALTY", "^CNXREALTY", "Nifty Realty", "NSE:CNXREALTY"),
    ("NIFTYENERGY", "^CNXENERGY", "Nifty Energy", "NSE:CNXENERGY"),
    ("NIFTYINFRA", "^CNXINFRA", "Nifty Infrastructure", "NSE:CNXINFRA"),
    ("NIFTYPSUBANK", "^CNXPSUBANK", "Nifty PSU Bank", "NSE:CNXPSUBANK"),
    ("NIFTYMEDIA", "^CNXMEDIA", "Nifty Media", "NSE:CNXMEDIA"),
    ("NIFTYPSE", "^CNXPSE", "Nifty PSE", "NSE:CNXPSE"),
    ("NIFTYFINSERV", "NIFTY_FIN_SERVICE.NS", "Nifty Financial Services", "NSE:CNXFINANCE"),
    ("NIFTYPVTBANK", "NIFTY_PVT_BANK.NS", "Nifty Private Bank", "NSE:NIFTYPVTBANK"),
    ("NIFTYHEALTH", "NIFTY_HEALTHCARE.NS", "Nifty Healthcare", "NSE:NIFTY_HEALTHCARE"),
    ("NIFTYCONSDUR", "NIFTY_CONSR_DURBL.NS", "Nifty Consumer Durables", "NSE:NIFTY_CONSR_DURBL"),
    ("NIFTYOILGAS", "NIFTY_OIL_AND_GAS.NS", "Nifty Oil & Gas", "NSE:NIFTY_OIL_AND_GAS"),
    ("NIFTYCOMMOD", "^CNXCMDT", "Nifty Commodities", "NSE:CNXCOMMODITIES"),
    ("NIFTYCONSUMP", "^CNXCONSUM", "Nifty India Consumption", "NSE:CNXCONSUMPTION"),
    ("INDIAVIX", "^INDIAVIX", "India VIX", "NSE:INDIAVIX"),
]
# Market overview instruments: (symbol, [Yahoo tickers to try in order], name, category, exchange)
# category: in = Indian indices, us = US indices, gl = global indices, cm = commodities/currencies/crypto, sx = sentiment
GLOBAL = [
    ("NIFTY50", ["^NSEI"], "NIFTY 50", "in", "NSE"),
    ("NIFTYNEXT50", ["^NSMIDCP"], "NIFTY NEXT 50", "in", "NSE"),
    ("BANKNIFTY", ["^NSEBANK"], "BANK NIFTY", "in", "NSE"),
    ("FINNIFTY", ["NIFTY_FIN_SERVICE.NS"], "FINNIFTY (Nifty Financial Services)", "in", "NSE"),
    ("NIFTYMID100", ["NIFTY_MIDCAP_100.NS", "^CRSMID"], "NIFTY MIDCAP 100", "in", "NSE"),
    ("NIFTYSML100", ["^CNXSC", "NIFTY_SMLCAP_100.NS"], "NIFTY SMALLCAP 100", "in", "NSE"),
    ("SENSEX", ["^BSESN"], "SENSEX", "in", "BSE"),
    ("BSE500", ["BSE-500.BO", "^BSE500"], "BSE 500", "in", "BSE"),
    ("INDIAVIX", ["^INDIAVIX"], "INDIA VIX", "in", "NSE"),
    ("IXIC", ["^IXIC"], "NASDAQ Composite", "us", "US"),
    ("NDX", ["^NDX"], "NASDAQ 100", "us", "US"),
    ("SPX", ["^GSPC"], "S&P 500", "us", "US"),
    ("DJI", ["^DJI"], "Dow Jones Industrial Average", "us", "US"),
    ("RUT", ["^RUT"], "Russell 2000", "us", "US"),
    ("SOX", ["^SOX"], "Philadelphia Semiconductor (SOX)", "us", "US"),
    ("N225", ["^N225"], "Nikkei 225 (Japan)", "gl", "JP"),
    ("SSEC", ["000001.SS"], "Shanghai Composite (China)", "gl", "CN"),
    ("HSI", ["^HSI"], "Hang Seng (Hong Kong)", "gl", "HK"),
    ("KOSPI", ["^KS11"], "KOSPI (South Korea)", "gl", "KR"),
    ("TWII", ["^TWII"], "Taiwan Weighted (Taiwan)", "gl", "TW"),
    ("FTSE", ["^FTSE"], "FTSE 100 (UK)", "gl", "UK"),
    ("DAX", ["^GDAXI"], "DAX (Germany)", "gl", "DE"),
    ("CAC", ["^FCHI"], "CAC 40 (France)", "gl", "FR"),
    ("ASX200", ["^AXJO"], "S&P/ASX 200 (Australia)", "gl", "AU"),
    ("STI", ["^STI"], "Straits Times (Singapore)", "gl", "SG"),
    ("TSX", ["^GSPTSE"], "S&P/TSX Composite (Canada)", "gl", "CA"),
    ("IBOV", ["^BVSP"], "Bovespa (Brazil)", "gl", "BR"),
    ("GOLD", ["GC=F"], "Gold (COMEX, USD/oz)", "cm", "FUT"),
    ("SILVER", ["SI=F"], "Silver (COMEX, USD/oz)", "cm", "FUT"),
    ("WTI", ["CL=F"], "Crude Oil WTI (USD/bbl)", "cm", "FUT"),
    ("BRENT", ["BZ=F"], "Crude Oil Brent (USD/bbl)", "cm", "FUT"),
    ("COPPER", ["HG=F"], "Copper (COMEX, USD/lb)", "cm", "FUT"),
    ("NATGAS", ["NG=F"], "Natural Gas (USD/MMBtu)", "cm", "FUT"),
    ("USDINR", ["INR=X", "USDINR=X"], "USD/INR", "cm", "FX"),
    ("DXY", ["DX-Y.NYB", "DX=F"], "US Dollar Index (DXY)", "cm", "FUT"),
    ("BTC", ["BTC-USD"], "Bitcoin (USD)", "cm", "CRYPTO"),
    ("VIX", ["^VIX"], "CBOE VIX (US volatility)", "sx", "US"),
    ("US10Y", ["^TNX"], "US 10-year yield (%)", "sx", "US"),
]
GLOBAL_PERIOD = "10y"
INTRADAY_INTERVAL = "5m"
LONG_PERIOD = "10y"
FUND_PER_RUN = 40          # fundamentals refreshed per run (cycles through all stocks)
FEEDS_EVERY_MIN = 60       # NSE feeds at most once an hour

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
    """Returns (symbol, yahoo ticker) pairs: stocks, then ETFs, then indices."""
    syms = read_list("ind_nifty200list.csv")
    n500 = read_list("ind_nifty500list.csv")
    print(f"Nifty 200 list: {len(syms) or 'built-in'}, Nifty 500 list: {len(n500) or 'not found'}, "
          f"ETFs: {len(ETFS)}, indices: {len(INDICES)}")
    if not syms:
        syms = list(NIFTY200_BUILTIN)
    extra = read_lines("extra_tickers.txt")
    pairs = [(x, x + ".NS") for x in syms + n500 + extra]
    pairs += [(e[0], e[1]) for e in ETFS] + [(i[0], i[1]) for i in INDICES]
    seen, out = set(), []
    for sym, yh in pairs:
        if sym not in seen:
            seen.add(sym)
            out.append((sym, yh))
    known = set(syms + n500) | {e[0] for e in ETFS} | {i[0] for i in INDICES}
    with open(os.path.join(OUT, "nse_groups.json"), "w") as f:
        json.dump({"etf": [{"sym": e[0], "name": e[2], "tv": e[3]} for e in ETFS],
                   "index": [{"sym": i[0], "name": i[2], "tv": i[3]} for i in INDICES],
                   "extra": extra_info([x for x in extra if x not in known])}, f, indent=1)
    return out


def extra_info(syms):
    """Name and sector for extra_tickers.txt stocks (from Yahoo), reusing what the site already has."""
    if not syms:
        return []
    prev = {}
    if SITE_URL:
        try:
            r = requests.get(f"{SITE_URL}/data/nse_groups.json", params={"t": time.time()}, timeout=30)
            if r.ok:
                prev = {x["sym"]: x for x in r.json().get("extra", [])}
        except Exception:
            pass
    out = []
    for sym in syms:
        if sym in prev and prev[sym].get("sector"):
            out.append(prev[sym])
            continue
        item = {"sym": sym, "name": "", "sector": ""}
        try:
            info = yf.Ticker(sym + ".NS").info or {}
            item["name"] = info.get("longName") or info.get("shortName") or ""
            item["sector"] = info.get("sector") or info.get("industry") or ""
        except Exception as e:
            print(f"  no sector info for {sym}: {e}")
        out.append(item)
    return out


def us_symbols():
    return [(x, x) for x in (read_lines("us_tickers.txt") or DEFAULT_US)]


def market_open_now(market):
    """Rough check in UTC, with a little margin. Holidays are ignored."""
    now = dt.datetime.now(dt.timezone.utc)
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    if market == "NSE":   # 09:15-15:30 IST = 03:45-10:00 UTC
        return 3 * 60 + 30 <= minutes <= 10 * 60 + 30
    return 13 * 60 + 15 <= minutes <= 21 * 60 + 15


def download(pairs, period, interval="1d"):
    frames, missing = [], []
    for start in range(0, len(pairs), 50):
        batch = [p[0] for p in pairs[start:start + 50]]
        tickers = [p[1] for p in pairs[start:start + 50]]
        data = None
        for attempt in range(3):
            try:
                data = yf.download(tickers, period=period, interval=interval, group_by="ticker",
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
            if interval == "1d":
                d["Date"] = pd.to_datetime(d["Date"]).dt.strftime("%Y-%m-%d")
            else:   # intraday: unix seconds (UTC)
                ts = pd.to_datetime(d["Date"], utc=True).dt.tz_convert(None)
                d["Date"] = ((ts - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1)).astype("int64")
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


PREV_META = None


def previous_meta_full():
    global PREV_META
    if PREV_META is None:
        PREV_META = {}
        if SITE_URL:
            try:
                r = requests.get(f"{SITE_URL}/data/meta.json", params={"t": time.time()}, timeout=30)
                if r.ok:
                    PREV_META = r.json()
            except Exception:
                pass
    return PREV_META


def previous_meta():
    return previous_meta_full().get("markets", {})


def fetch_prev_bytes(name, timeout=180):
    if not SITE_URL:
        return None
    try:
        r = requests.get(f"{SITE_URL}/data/{name}", params={"t": time.time()}, timeout=timeout)
        return r.content if r.ok else None
    except Exception:
        return None


def fetch_prev_json(name):
    b = fetch_prev_bytes(name, 60)
    try:
        return json.loads(b) if b else None
    except Exception:
        return None


def global_pairs():
    return [(g[0], g[1][0]) for g in GLOBAL]


def download_global(period, interval="1d"):
    """Download overview instruments, trying alternative Yahoo tickers when the first one has no data."""
    df, n = download(global_pairs(), period, interval)
    have = set(df["Symbol"]) if df is not None else set()
    extra = []
    for sym, tks, *_ in GLOBAL:
        if sym in have:
            continue
        for alt in tks[1:]:
            d2, n2 = download([(sym, alt)], period, interval)
            if d2 is not None and n2:
                extra.append(d2)
                break
    if extra:
        df = pd.concat([df] + extra) if df is not None else pd.concat(extra)
    return df, (df["Symbol"].nunique() if df is not None else 0)


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


def check_alerts():
    """Send a Telegram message for each alert in alerts.csv whose price condition is met (once per alert)."""
    token, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not os.path.exists("alerts.csv"):
        return
    if not token or not chat:
        print("alerts.csv found, but TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID secrets are not set")
        return
    alerts = pd.read_csv("alerts.csv")
    alerts.columns = [c.strip().lower() for c in alerts.columns]
    latest, series = {}, {}
    want = {(str(a.get("market", "NSE")).strip().upper() or "NSE", str(a.get("symbol", "")).strip().upper()) for _, a in alerts.iterrows()}
    for key, mk in (("nse", "NSE"), ("us", "US"), ("glb", "GLB")):
        frames = [pd.read_csv(os.path.join(OUT, n)) for n in (f"{key}_history.csv", f"{key}_recent.csv") if os.path.exists(os.path.join(OUT, n))]
        if frames:
            allr = pd.concat(frames)
            for (m, sym) in want:
                if m == mk:
                    g = allr[allr["Symbol"] == sym].drop_duplicates("Date", keep="last").sort_values("Date")
                    if len(g):
                        series[(m, sym)] = g
    for key, mk in (("nse", "NSE"), ("us", "US")):
        for name in (f"{key}_history.csv", f"{key}_recent.csv"):
            path = os.path.join(OUT, name)
            if not os.path.exists(path):
                continue
            df = pd.read_csv(path)
            if df.empty:
                continue
            for _, r in df.sort_values("Date").groupby("Symbol").tail(1).iterrows():
                prev = latest.get((mk, r["Symbol"]))
                if not prev or r["Date"] >= prev[0]:
                    latest[(mk, r["Symbol"])] = (r["Date"], float(r["Close"]))
    state = {"fired": {}}
    if SITE_URL:
        try:
            r = requests.get(f"{SITE_URL}/data/alerts_state.json", params={"t": time.time()}, timeout=30)
            if r.ok:
                state = r.json()
        except Exception:
            pass
    fired = state.setdefault("fired", {})
    sent = 0
    for _, a in alerts.iterrows():
        sym = str(a.get("symbol", "")).strip().upper()
        mk = str(a.get("market", "NSE")).strip().upper() or "NSE"
        cond = str(a.get("condition", "")).strip().lower()
        try:
            price = float(a.get("price"))
        except (TypeError, ValueError):
            continue
        key = f"{mk}|{sym}|{cond}|{price}"
        if key in fired:
            continue
        g = series.get((mk, sym))
        if g is None and (mk, sym) not in latest:
            continue
        day, close = (g["Date"].iloc[-1], float(g["Close"].iloc[-1])) if g is not None else latest[(mk, sym)]
        met, what = False, ""
        if cond in ("above", "below"):
            met = (cond == "above" and close >= price) or (cond == "below" and close <= price)
        elif g is not None and len(g) > 30:
            c = g["Close"].astype(float).reset_index(drop=True)
            d = c.diff()
            up, dn = d.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean(), (-d.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
            rsi = 100 - 100 / (1 + up / dn.replace(0, 1e-9))
            if cond == "rsi_below":
                met, what = rsi.iloc[-1] < price <= rsi.iloc[-2], f"RSI fell below {price:g} (now {rsi.iloc[-1]:.1f})"
            elif cond == "rsi_above":
                met, what = rsi.iloc[-1] > price >= rsi.iloc[-2], f"RSI rose above {price:g} (now {rsi.iloc[-1]:.1f})"
            elif cond == "breakout20":
                hi = g["High"].astype(float).reset_index(drop=True).iloc[-21:-1].max()
                met, what = close > hi, f"closed above its 20-day high of {hi:,.2f}"
            elif cond in ("below20sma", "below50sma"):
                per = 20 if cond == "below20sma" else 50
                m = c.rolling(per).mean()
                met, what = c.iloc[-1] < m.iloc[-1] and c.iloc[-2] >= m.iloc[-2], f"closed below its {per}-day SMA ({m.iloc[-1]:,.2f})"
            elif cond == "golden_cross" and len(c) > 205:
                s50, s200 = c.rolling(50).mean(), c.rolling(200).mean()
                met, what = s50.iloc[-1] > s200.iloc[-1] and s50.iloc[-2] <= s200.iloc[-2], "50-day average crossed above the 200-day average"
        if met and what:
            text = f"Chartviewer alert: {sym} {what}. Price {close:,.2f}.\nTechnical alert only, not investment advice."
            try:
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat, "text": text}, timeout=30)
                fired[key] = day
                sent += 1
            except Exception as e:
                print(f"  couldn't send Telegram alert for {sym}: {e}")
            continue
        if met:
            note = a.get("note")
            note = f" ({note})" if isinstance(note, str) and note.strip() else ""
            text = (f"Chartviewer alert: {sym} is {close:,.2f}, "
                    f"{'at or above' if cond == 'above' else 'at or below'} your level {price:,.2f}{note}.\n"
                    f"Technical alert only, not investment advice.")
            try:
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                              data={"chat_id": chat, "text": text}, timeout=30)
                fired[key] = day
                sent += 1
            except Exception as e:
                print(f"  couldn't send Telegram alert for {sym}: {e}")
    with open(os.path.join(OUT, "alerts_state.json"), "w") as f:
        json.dump(state, f, indent=1)
    print(f"Alerts: {len(alerts)} checked, {sent} sent")


# ---------- every other NSE security (end of day, from NSE bhavcopy) ----------
BHAV_URL = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{}_F_0000.csv.zip"
EQ_SERIES = {"EQ", "BE", "BZ", "SM", "ST"}
DEBT_SERIES = {"GB": "sgb", "GS": "gsec", "SG": "sdl", "TB": "tbill"}
EOD_DAYS = 370          # keep about one year of history
EOD_MAX_FETCH = 300     # bhavcopies to download in one run (first run backfills)


def series_kind(series):
    if series in EQ_SERIES:
        return "eq"
    if series in DEBT_SERIES:
        return DEBT_SERIES[series]
    if len(series) == 2 and series[0] in "NY":
        return "bond"
    return None


def fetch_bhav(session, day):
    try:
        r = session.get(BHAV_URL.format(day.strftime("%Y%m%d")), timeout=30)
        if r.status_code != 200 or r.content[:2] != b"PK":
            return None, r.status_code
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            df = pd.read_csv(z.open(z.namelist()[0]))
    except Exception as e:
        return None, str(e)
    df.columns = [c.strip() for c in df.columns]
    df["SctySrs"] = df["SctySrs"].astype(str).str.strip()
    df["kind"] = df["SctySrs"].map(series_kind)
    df = df[df["kind"].notna()]
    out = pd.DataFrame({
        "Date": day.isoformat(), "Symbol": df["TckrSymb"].astype(str).str.strip(), "Series": df["SctySrs"],
        "Name": df.get("FinInstrmNm", "").astype(str).str.strip(), "ISIN": df.get("ISIN", "").astype(str).str.strip(),
        "Open": df["OpnPric"], "High": df["HghPric"], "Low": df["LwPric"], "Close": df["ClsPric"],
        "Volume": df["TtlTradgVol"].fillna(0).astype("int64")})
    return out, 200


def safe_name(sym):
    return re.sub(r"[^A-Za-z0-9]", "_", sym)


def eod_update(main_syms):
    """Keep data/eod_history.csv.gz (one year, all NSE securities outside the main bundle) up to date,
    and write one small file per security in data/x/ plus data/others_index.json for the page."""
    prev = None
    if SITE_URL:
        try:
            r = requests.get(f"{SITE_URL}/data/eod_history.csv.gz", params={"t": time.time()}, timeout=120)
            if r.ok and r.content[:2] == b"\x1f\x8b":
                prev = pd.read_csv(io.BytesIO(r.content), compression="gzip", dtype={"Series": str})
        except Exception as e:
            print(f"EOD: couldn't load previous history: {e}")
    now = dt.datetime.now(dt.timezone.utc)
    today_ist = (now + dt.timedelta(hours=5, minutes=30)).date()
    after_close = (now + dt.timedelta(hours=5, minutes=30)).time() >= dt.time(18, 30)
    start = today_ist - dt.timedelta(days=EOD_DAYS)
    have = set(prev["Date"].unique()) if prev is not None else set()
    last = max(have) if have else None
    days, d = [], today_ist
    while d > start and len(days) < EOD_MAX_FETCH:
        if d.weekday() < 5 and d.isoformat() not in have and (d < today_ist or after_close):
            if last and d.isoformat() < last:
                break          # only fetch days after what we already have
            days.append(d)
        d -= dt.timedelta(days=1)
    frames = [prev] if prev is not None else []
    if days:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                                "Referer": "https://www.nseindia.com/", "Accept": "*/*"})
        got, blocked = 0, 0
        print(f"EOD: downloading up to {len(days)} NSE bhavcopies…")
        for d in days:
            df, status = fetch_bhav(session, d)
            if df is None:
                if status in (401, 403):
                    blocked += 1
                    if blocked >= 3 and not got:
                        print("EOD: NSE is refusing downloads from this server (HTTP 403).")
                        break
                continue
            frames.append(df)
            got += 1
            time.sleep(0.4)
        print(f"EOD: {got} new bhavcopies")
    if not frames:
        print("EOD: no data for securities outside the Nifty 500")
        return
    allf = pd.concat(frames, ignore_index=True)
    allf = allf[allf["Date"] >= start.isoformat()]
    allf = allf.drop_duplicates(["Date", "Symbol", "Series"], keep="last")
    with gzip.open(os.path.join(OUT, "eod_history.csv.gz"), "wt") as f:
        allf.to_csv(f, index=False)
    # one series per symbol: the one with the most recent data (an equity can move EQ <-> BE)
    allf = allf[~((allf["Series"].map(series_kind) == "eq") & allf["Symbol"].isin(main_syms))]
    lastrow = allf.sort_values("Date").groupby("Symbol").tail(1).set_index("Symbol")
    xdir = os.path.join(OUT, "x")
    os.makedirs(xdir, exist_ok=True)
    index = []
    for sym, g in allf.groupby("Symbol"):
        lr = lastrow.loc[sym]
        g = g[g["Series"] == lr["Series"]].sort_values("Date")
        if g.empty:
            continue
        fname = safe_name(sym) + ".csv"
        g[["Date", "Symbol", "Open", "High", "Low", "Close", "Volume"]].to_csv(os.path.join(xdir, fname), index=False)
        prevc = float(g["Close"].iloc[-2]) if len(g) > 1 else None
        index.append({"sym": sym, "name": lr["Name"], "series": lr["Series"], "kind": series_kind(lr["Series"]),
                      "isin": lr["ISIN"], "file": "x/" + fname, "c": float(lr["Close"]), "pc": prevc, "t": lr["Date"]})
    with open(os.path.join(OUT, "others_index.json"), "w") as f:
        json.dump({"updated": now_iso(), "items": index}, f, separators=(",", ":"))
    kinds = {}
    for it in index:
        kinds[it["kind"]] = kinds.get(it["kind"], 0) + 1
    print(f"EOD: {len(index)} other securities published {kinds}")


# ---------- intraday (5-minute bars, last 5 sessions) and long daily history ----------
def write_split(df, folder, time_col="Date"):
    os.makedirs(folder, exist_ok=True)
    n = 0
    for sym, g in df.groupby("Symbol"):
        g.sort_values(time_col).to_csv(os.path.join(folder, safe_name(str(sym)) + ".csv"), index=False)
        n += 1
    return n


def bundle(name, df=None):
    """Write df to data/<name>.csv.gz, or load the previous one from the site when df is None."""
    path = os.path.join(OUT, name + ".csv.gz")
    if df is not None:
        with gzip.open(path, "wt") as f:
            df.to_csv(f, index=False)
        return df
    b = fetch_prev_bytes(name + ".csv.gz", 300)
    if not b or b[:2] != b"\x1f\x8b":
        return None
    with open(path, "wb") as f:
        f.write(b)
    return pd.read_csv(io.BytesIO(b), compression="gzip")


def intraday_update(mode, nse_pairs, us_pairs):
    prevx = (previous_meta_full().get("extras") or {}).get("intraday", {})
    out = {}
    groups = (("nse", nse_pairs, "NSE"), ("us", us_pairs, "US"), ("glb", None, "GLB"))
    for key, pairs, market in groups:
        fresh = mode != "auto" or market == "GLB" or market_open_now(market) or key not in prevx
        df = None
        if fresh:
            print(f"Intraday {key}: downloading {INTRADAY_INTERVAL} bars…")
            df, n = download_global("5d", INTRADAY_INTERVAL) if market == "GLB" else download(pairs, "5d", INTRADAY_INTERVAL)
            if df is not None and n:
                bundle(f"{key}_intraday", df)
                out[key] = {"updatedAt": now_iso(), "symbols": int(n), "interval": INTRADAY_INTERVAL}
            else:
                df = None
        if df is None:
            df = bundle(f"{key}_intraday")
            if df is not None:
                out[key] = prevx.get(key, {"updatedAt": None})
        if df is not None:
            print(f"Intraday {key}: {write_split(df, os.path.join(OUT, 'i', key))} files")
    return out


def long_update(mode, nse_pairs, us_pairs):
    """Up to 10 years of daily bars per stock for long chart ranges; refreshed once a week."""
    prevx = (previous_meta_full().get("extras") or {}).get("long", {})
    out = {}
    week = dt.date.today().isocalendar()[1]
    for key, pairs in (("nse", nse_pairs), ("us", us_pairs)):
        old = prevx.get(key, {})
        df = None
        if mode == "all" or old.get("week") != week:
            print(f"Long history {key}: downloading {LONG_PERIOD}…")
            df, n = download(pairs, LONG_PERIOD)
            if df is not None and n >= 0.5 * len(pairs):
                bundle(f"{key}_long", df)
                out[key] = {"updatedAt": now_iso(), "week": week, "symbols": int(n)}
            else:
                df = None
        if df is None:
            df = bundle(f"{key}_long")
            if df is not None:
                out[key] = old
        if df is not None:
            print(f"Long history {key}: {write_split(df, os.path.join(OUT, 'l', key))} files")
            if key == "nse":
                try:
                    m = df.sort_values(["Symbol", "Date"]).copy()
                    m["Month"] = m["Date"].astype(str).str[:7]
                    g = m.groupby(["Symbol", "Month"]).agg(Open=("Open", "first"), High=("High", "max"), Low=("Low", "min"),
                                                         Close=("Close", "last"), Volume=("Volume", "sum")).reset_index()
                    g.round(2).to_csv(os.path.join(OUT, "nse_monthly.csv"), index=False)
                    print(f"Monthly candles: {len(g)} rows for {g['Symbol'].nunique()} stocks")
                except Exception as e:
                    print(f"  monthly candles failed: {e}")
            LISTINGS[key] = {str(k): str(v) for k, v in df.groupby("Symbol")["Date"].min().items()}
    with open(os.path.join(OUT, "listings.json"), "w") as f:
        json.dump({"note": f"First trading day within the last {LONG_PERIOD} of data", "period": LONG_PERIOD, **LISTINGS}, f, separators=(",", ":"))
    return out


LISTINGS = {}


# ---------- fundamentals (Yahoo Finance, refreshed a few dozen stocks per run) ----------
def _num(v, scale=1.0):
    try:
        v = float(v)
        return None if v != v else round(v * scale, 4)
    except (TypeError, ValueError):
        return None


def _row(df, names):
    if df is None or getattr(df, "empty", True):
        return None
    for nm in names:
        if nm in df.index:
            return df.loc[nm]
    return None


def fetch_fundamentals(sym, ticker):
    t = yf.Ticker(ticker)
    info = t.info or {}
    if not info or (info.get("regularMarketPrice") is None and info.get("marketCap") is None):
        return None
    price = info.get("currentPrice") or info.get("regularMarketPrice")
    dy = None
    if info.get("dividendRate") and price:
        dy = round(float(info["dividendRate"]) / float(price) * 100, 2)
    elif info.get("trailingAnnualDividendYield") is not None:
        dy = _num(info.get("trailingAnnualDividendYield"), 100)
    de = _num(info.get("debtToEquity"))
    f = {"mcap": _num(info.get("marketCap")), "pe": _num(info.get("trailingPE")), "fpe": _num(info.get("forwardPE")),
         "pb": _num(info.get("priceToBook")), "eps": _num(info.get("trailingEps")), "roe": _num(info.get("returnOnEquity"), 100),
         "de": round(de / 100, 2) if de is not None else None, "dy": dy,
         "revg": _num(info.get("revenueGrowth"), 100), "epsg": _num(info.get("earningsGrowth"), 100),
         "opm": _num(info.get("operatingMargins"), 100), "npm": _num(info.get("profitMargins"), 100),
         "insiders": _num(info.get("heldPercentInsiders"), 100), "inst": _num(info.get("heldPercentInstitutions"), 100),
         "beta": _num(info.get("beta")), "industry": info.get("industry"), "sector": info.get("sector"),
         "cur": info.get("financialCurrency") or info.get("currency"), "exDiv": info.get("exDividendDate"),
         "earn": info.get("earningsTimestamp")}
    q = []
    try:
        qs = t.quarterly_income_stmt
        rev, ni = _row(qs, ["Total Revenue", "Operating Revenue"]), _row(qs, ["Net Income", "Net Income Common Stockholders"])
        if rev is not None:
            for col in list(qs.columns)[:6]:
                q.append({"d": str(col)[:10], "rev": _num(rev.get(col)), "np": _num(ni.get(col)) if ni is not None else None})
    except Exception:
        pass
    a = []
    try:
        an, bs = t.income_stmt, t.balance_sheet
        rev, ni = _row(an, ["Total Revenue", "Operating Revenue"]), _row(an, ["Net Income", "Net Income Common Stockholders"])
        ebit = _row(an, ["EBIT", "Operating Income"])
        ta, cl = _row(bs, ["Total Assets"]), _row(bs, ["Current Liabilities", "Total Current Liabilities"])
        if rev is not None:
            for col in list(an.columns)[:5]:
                a.append({"y": str(col)[:4], "rev": _num(rev.get(col)), "np": _num(ni.get(col)) if ni is not None else None,
                          "ebit": _num(ebit.get(col)) if ebit is not None else None})
        if ebit is not None and ta is not None and cl is not None:
            col = list(an.columns)[0]
            ce = _num(ta.get(col)) - _num(cl.get(col)) if col in ta.index and col in cl.index else None
            if ce and ebit.get(col) == ebit.get(col):
                f["roce"] = round(float(ebit.get(col)) / ce * 100, 2)
        if len(a) >= 2 and a[0]["rev"] and a[1]["rev"]:
            f["revg_y"] = round((a[0]["rev"] / a[1]["rev"] - 1) * 100, 2)
        if len(a) >= 2 and a[0]["np"] and a[1]["np"] and a[1]["np"] > 0:
            f["npg_y"] = round((a[0]["np"] / a[1]["np"] - 1) * 100, 2)
    except Exception:
        pass
    f["q"], f["a"] = q, a
    f["updated"] = now_iso()
    return {k: v for k, v in f.items() if v not in (None, [], "")}


def fundamentals_update(nse_pairs, us_pairs, per_run=FUND_PER_RUN):
    prev = fetch_prev_json("fundamentals.json") or {"items": {}}
    items = prev.get("items", {})
    etf_idx = {e[0] for e in ETFS} | {i[0] for i in INDICES}
    stocks = [(s, y) for s, y in nse_pairs if s not in etf_idx] + [("US:" + s, y) for s, y in us_pairs]
    stocks.sort(key=lambda p: (items.get(p[0], {}).get("updated") or ""))
    done = 0
    for key, ticker in stocks[:per_run]:
        try:
            f = fetch_fundamentals(key, ticker)
            if f:
                items[key] = f
                done += 1
        except Exception as e:
            msg = str(e)
            print(f"  fundamentals {key}: {msg[:80]}")
            if "Rate" in msg or "Too Many" in msg:
                break
        time.sleep(0.5)
    with open(os.path.join(OUT, "fundamentals.json"), "w") as f:
        json.dump({"updatedAt": now_iso(), "source": "Yahoo Finance", "items": items}, f, separators=(",", ":"))
    print(f"Fundamentals: {done} refreshed, {len(items)} stocks in total")


# ---------- NSE feeds: delivery %, bulk/block deals, FII/DII, corporate actions, announcements, results calendar ----------
def nse_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0 Safari/537.36", "Accept": "application/json, text/plain, */*",
                      "Accept-Language": "en-US,en;q=0.9", "Referer": "https://www.nseindia.com/"})
    try:
        s.get("https://www.nseindia.com/", timeout=20)
    except Exception:
        pass
    return s


def nse_feeds():
    prev = fetch_prev_json("nse_feeds.json") or {}
    last = prev.get("updatedAt")
    if last:
        age = (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(last)).total_seconds() / 60
        if age < FEEDS_EVERY_MIN:
            with open(os.path.join(OUT, "nse_feeds.json"), "w") as f:
                json.dump(prev, f, separators=(",", ":"))
            print("NSE feeds: recent copy kept")
            return
    s = nse_session()
    out = {"updatedAt": now_iso(), "status": {}}

    def keep(name, fn):
        try:
            out[name] = fn()
            out["status"][name] = "ok"
        except Exception as e:
            out[name] = prev.get(name)
            out["status"][name] = f"failed: {str(e)[:120]}"
        print(f"  NSE {name}: {out['status'][name]}")

    def delivery():
        d = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)
        for back in range(0, 7):
            day = (d - dt.timedelta(days=back)).date()
            if day.weekday() >= 5:
                continue
            r = s.get(f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{day.strftime('%d%m%Y')}.csv", timeout=30)
            if r.ok and "DELIV_PER" in r.text[:400]:
                df = pd.read_csv(io.StringIO(r.text))
                df.columns = [c.strip() for c in df.columns]
                df["SERIES"] = df["SERIES"].astype(str).str.strip()
                df = df[df["SERIES"].isin(["EQ", "BE"])]
                dp = pd.to_numeric(df["DELIV_PER"].astype(str).str.strip(), errors="coerce")
                return {"date": day.isoformat(), "items": {str(k).strip(): round(float(v), 2)
                        for k, v in zip(df["SYMBOL"], dp) if v == v}}
        raise RuntimeError(f"no file (HTTP {r.status_code})")

    def deals(kind):
        r = s.get(f"https://nsearchives.nseindia.com/content/equities/{kind}.csv", timeout=30)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = [c.strip() for c in df.columns]
        return df.head(400).fillna("").astype(str).to_dict("records")

    def api(path):
        r = s.get("https://www.nseindia.com/api/" + path, timeout=30)
        r.raise_for_status()
        return r.json()

    def fiidii():
        rows = api("fiidiiTradeReact")
        hist = {h["date"] + h["category"]: h for h in (prev.get("fiidii") or [])}
        for h in rows:
            hist[str(h.get("date")) + str(h.get("category"))] = {k: h.get(k) for k in ("category", "date", "buyValue", "sellValue", "netValue")}
        return sorted(hist.values(), key=lambda h: dt.datetime.strptime(h["date"], "%d-%b-%Y"), reverse=True)[:120]

    def corp_actions():
        rows = api("corporates-corporateActions?index=equities")
        today = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).date()
        out_rows = []
        for r in rows:
            try:
                ex = dt.datetime.strptime(r.get("exDate", ""), "%d-%b-%Y").date()
            except ValueError:
                continue
            if ex >= today - dt.timedelta(days=7):
                out_rows.append({"sym": r.get("symbol"), "name": r.get("comp"), "subject": r.get("subject"),
                                 "exDate": ex.isoformat(), "recDate": r.get("recDate")})
        return sorted(out_rows, key=lambda x: x["exDate"])[:300]

    def announcements():
        rows = api("corporate-announcements?index=equities")
        return [{"sym": r.get("symbol"), "name": r.get("sm_name"), "desc": r.get("desc"), "text": (r.get("attchmntText") or "")[:240],
                 "time": r.get("an_dt"), "file": r.get("attchmntFile")} for r in rows[:120]]

    def events():
        rows = api("event-calendar")
        return [{"sym": r.get("symbol"), "name": r.get("company"), "purpose": r.get("purpose"), "date": r.get("date"),
                 "desc": (r.get("bm_desc") or "")[:200]} for r in rows[:300]]

    def ipos():
        cur = api("ipo-current-issue")
        return [{"sym": r.get("symbol"), "name": r.get("companyName"), "series": r.get("series"), "open": r.get("issueStartDate"),
                 "close": r.get("issueEndDate"), "price": r.get("issuePrice"), "size": r.get("issueSize"),
                 "subscribed": r.get("noOfTime")} for r in (cur if isinstance(cur, list) else [])]

    keep("delivery", delivery)
    keep("bulk", lambda: deals("bulk"))
    keep("block", lambda: deals("block"))
    keep("fiidii", fiidii)
    keep("corpActions", corp_actions)
    keep("announcements", announcements)
    keep("events", events)
    keep("ipos", ipos)
    with open(os.path.join(OUT, "nse_feeds.json"), "w") as f:
        json.dump(out, f, separators=(",", ":"))


# ---------- market headlines (RSS: titles and links only) ----------
NEWS_FEEDS = [
    ("Economic Times", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("Moneycontrol", "https://www.moneycontrol.com/rss/marketreports.xml"),
    ("Livemint", "https://www.livemint.com/rss/markets"),
    ("Business Standard", "https://www.business-standard.com/rss/markets-106.rss"),
]


def news_update():
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime
    items, status = [], {}
    for src, url in NEWS_FEEDS:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            n = 0
            for it in root.iter("item"):
                title = (it.findtext("title") or "").strip()
                link = (it.findtext("link") or "").strip()
                pub = it.findtext("pubDate")
                try:
                    ts = parsedate_to_datetime(pub).astimezone(dt.timezone.utc).isoformat() if pub else None
                except Exception:
                    ts = None
                if title and link.startswith("http"):
                    items.append({"src": src, "title": title[:240], "link": link, "time": ts})
                    n += 1
            status[src] = f"ok ({n})"
        except Exception as e:
            status[src] = f"failed: {str(e)[:80]}"
    items.sort(key=lambda x: x["time"] or "", reverse=True)
    with open(os.path.join(OUT, "news.json"), "w") as f:
        json.dump({"updatedAt": now_iso(), "status": status, "items": items[:80]}, f, separators=(",", ":"))
    print(f"News: {status}")


# ---------- data quality: Yahoo close vs NSE official close, and unusual jumps ----------
def quality_check(nse_pairs):
    out = {"updatedAt": now_iso(), "compared": 0, "mismatches": [], "jumps": {}, "nseDate": None}
    frames = []
    for name in ("nse_history.csv", "nse_recent.csv"):
        path = os.path.join(OUT, name)
        if os.path.exists(path):
            frames.append(pd.read_csv(path))
    if not frames:
        return
    y = pd.concat(frames).drop_duplicates(["Date", "Symbol"], keep="last").sort_values(["Symbol", "Date"])
    skip = {e[0] for e in ETFS} | {i[0] for i in INDICES}
    for sym, g in y[~y["Symbol"].isin(skip)].groupby("Symbol"):
        r = g["Close"].pct_change() * 100
        big = g.loc[r.abs() > 20, "Date"].tolist()
        if big:
            out["jumps"][sym] = [[d, round(float(v), 1)] for d, v in zip(big, r[r.abs() > 20].tolist())][-5:]
    eod = os.path.join(OUT, "eod_history.csv.gz")
    if os.path.exists(eod):
        e = pd.read_csv(eod, compression="gzip", dtype={"Series": str})
        e = e[e["Series"].isin(["EQ", "BE"])]
        if not e.empty:
            day = e["Date"].max()
            out["nseDate"] = day
            nse = e[e["Date"] == day].set_index("Symbol")["Close"]
            yy = y[y["Date"] == day].set_index("Symbol")["Close"]
            both = nse.index.intersection(yy.index)
            out["compared"] = int(len(both))
            for sym in both:
                a, b = float(yy[sym]), float(nse[sym])
                if b and abs(a / b - 1) > 0.005:
                    out["mismatches"].append({"sym": sym, "date": day, "yahoo": round(a, 2), "nse": round(b, 2), "diff": round((a / b - 1) * 100, 2)})
    with open(os.path.join(OUT, "quality.json"), "w") as f:
        json.dump(out, f, separators=(",", ":"))
    print(f"Quality: {out['compared']} closes compared with NSE, {len(out['mismatches'])} mismatches, {len(out['jumps'])} stocks with >20% jumps")


# ---------- installable app (manifest + icons) ----------
ICON_192 = "iVBORw0KGgoAAAANSUhEUgAAAMAAAADACAMAAABlApw1AAAAwFBMVEX////7+/z09fbv8PLj5ek+4KHd3+PY2t/P0tjHytC2usOytr+iqLN6ov882p472Jw815w81pw605o5z5g2vY8zsYiQlqR9hJRqcoQvmnwtk3lmboElbWX/a1v2Z1nvZVjhYVe2VFKBQ0tjUmJcOEdbOEdTXHFJU2k6RVxWN0YzPlcuOlNONEY+LkMoNE4rKUEjZGAhXF0hW1wiSlUiL0kiLEYfU1geTFQcQ1AYMUYcKUQZJUEWJUAVJUAYIz8WIz+C69n2AAADHUlEQVR42u3b6VLiQBQF4HYJS4VRx4WMQgihGFHEOFIUCCh5/7caJJksTQeyDObe8pxflmhVvqTTfXtBuMwjAAAAAAAAAAAAAAAAAAAAAAAAAAAKZDhkDXDqQtQdvgCnItY5dbgCFjWxSW3BFKALPzpPQFMEaXIEtEQkLX6AgYhlwA0wPIkDToa8AJOKkFKZcAIs62Ir9SUjQCO47EbkRz4AI3LbIw/D4AK4P4o2/PB1OLrnAXg4jnc9YYd0/MAB4JwGd9zv/AfBE8lS15UF+FfBRYffcFDOUNeVBdBVBVBYFunUAQmXqmev68oByI3l4+3tI6lhUQTIr2v7UtMu/6hfbYoAucOcn2nr/JyrOleSgLCdeEPWVNtk6g1v4fBMFdCSi4ZXDzCWC4wWUUBDLtvigPDzJlGAIRfOEiCo634RBfhlWzWYukgAd1LNMLcppRf6fIurT24SwH36FNTo9kLr91g3Ird3C+BODJ30QCZlG8BqYQuAMtO+vtBiOb9uMwK832mK3L2zAbxqyozZAG7UgBs2gDM14JwNQEsIAAAAAAAAAAAAAABFAT8iOdAvdwCU/w4AAAB8KwDGAQAAAMANt+C5AsIteJ6A6BY8S0BsC54jYM/WCwAAAAAAAABQBaTagqcLSLkFTxeQcgueLiDlFjxdQMIW/BUbQMorAwAAAAAAAIDSAKvn5xVnQM8yTavPF/DSMdfpvLAFjMxNRmwBfQ/wGwAAAAAAAADyA0r5AkSv2zFj6XR7eQFj9V9NDwhY2qYi9ipn21CvQLoHBPRNZR7zNu7Z7ZW09nU7cw8JsNUAu9jb+YVfhLPUAIsNwEwIAAAAAAAAAAAAAAAAAPC/ABln6/QAGWfr/gHfGR1Axtm6d8T6Yk4IkHG2vvuQeymAjI1759cMOAAKBAAAvhTQTbcyRxfwmG5tlC5AvTia9AAoAtyRLa2PWvbI5QTwsmeHBgAAAACABqDQVIsCoNBUiwKg0FSLBKDIVIsG4PD5poA9xy7pA/yDrwu2gD1HjxkAdh/+5gAgEwAAAAAAAAAAAAAAAAAAAAAAyJm/lRg9uHQZslAAAAAASUVORK5CYII="
ICON_512 = "iVBORw0KGgoAAAANSUhEUgAAAgAAAAIACAMAAADDpiTIAAAAwFBMVEX////+/v76+vv29/jw8fPp6u3i4+c+4KHd3+PV19zJzNK6vsaqrrh6ov8825471Zs4yZU2v5A2vI41uo01uow1uY00sokxp4ORl6SAh5YwoIBpcYMumXwriXQofG3/a1ska2TvZljYXlbQXFTPXFXEWFOxUlGPS1ByP0pbZHhKVGpUO0sjYmAgWVtAS2IiSFQ1QFksOFEaOks0LUQlL0kiL0keKkUfJUAYLUQZJ0EXJUAVJUAYIz8WIz8VIz8WIj6gFd5SAAAKvUlEQVR42u3deVcaWRrAYbqTbhN1FJyIKwExgMa4BNLtWj3f/1tNljkZJYrUInWr7vP7Oy19vI+13JdT1UgUdQ2/AgAEgAAQAAJAAAgAASAABIAAEAACQAAIAAEgAASAABAAAkAACAABIAAEgAAQAAJAAAgAASAABIAAEAACQAAIAAEgAASAABAAAkAACAABIAAEgAAQAAJAAAgAASAABIAAEADV6/PezvEZAJF2vPq28a3XOwBE2N8rjZ8tXwAQWxdvGvc7BiCuzl4/WP/GqzMAorr4+6Mx1R+fAYjo/P+28Utv/wYgmlYaj7QCQCxtNx5tG4A42ms80R4AMfTx96cA/P4RgAg2AF43nuz1BQB178ubxoze/AVAvbtebsxs+QaAWrfaeKZVAOrcTuPZdgCob8e/PQ/gt2MA6trZq8YchTEXAqD4fp0APV4QcyEACu+xCdDjhTAXAqDwVhpztwJA/dpupGgbgLq110jVHgD16ukJUKBzIQAKbdYEKMy5EABFNnsCFORcCIACe24CFOJcCIACmzEBWlkJdC4EQHHtzNrymbU9tANAHZoxAfq26Ttjg7jMuRAARTVjAvRj7PPsPwCgys3xB/7MIQKAKjfXKX4nwLkQAMU030X+anhzIQAKacYEaPl6vo2CbQCq2968G32ztgr3AKhqMyZA01v9M4YF5cyFAMhfqkVNgQWAapTysL4X1lwIgLylvrDbDmouBEDe0t/aBTUXAiBnGTZ3gpoLAZCvTNu7Ic2FAMhVxgFPQHMhAPKU+U85nLkQADnKcTIPZi4EQI7yXM6HMhcCIHtzToAK2j4AILRybukFMhcCIGu5N/XDmAsBkLFUy3fe29/vnRdOCIDySnMAP20ufa95WuxJBIDySnEJN95a+tnWeP7LyBsAAi7FTdy99f8qoMAbSQDKK8U2Tm/pQb3CtpIAKK8UG7nj9YcA1qdOAqXPhQBI3+cUo5z3S1O9n/oHs+ZCnwGo2AXAr3+0+9MA9lMcTlYBCLGzNKft9jSAdpoLijMAAmw5zV/su2kAm2mOKMsABHgFkGoCNA+AGZsKnwGozhng0c27eQDM2FY8AyC8e8BU2/dzAXh6sPARgODaS7VW8wFIPj7xQ48BqMop4IkR/pwAnmJ1AUB4/ZnmSzzzAnh8LvSnu4CK7AM9+TW+uQE8OhdaBaAaFwFPf5F3fgCPzYWOAQiwX2/bZ3yVf34Aj8yFFvGdAAAybAVNXQW8nnG3ngJAcjZ1M/jnlwSAMG8EHkzw3sy6VE8DILl4sCH0aiHfCwQgSxf3rthWZn57LxWA5K97P3d1Md8LBSBbH39cCCzvPLNZmw7A14PLzo8fvLKoL4YDkN3Azt7zJ+m0AL72ZW9ngY+LAuBlywBgsQEAgAAQAAJAAAgAASAAlLHzTru5tjRXa8125xyAOnXTay2lrNW7BaAuXW0uZah9BUA9GjeXMtUaA1CHrjeWMrZxDUAN2l3K3C4A1e90KUenAFT+BqCVB0DrBoCYDwAlHgIAKKhOPgAdACpeOx+ANgAVr5kPQBOAil8DruUDsAZAxVvKGQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMALAPjXM8Xy3xUFIOv/JwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAUCUDFAlh0AAAgAASAABAAAkAACAABIAAEgAAQAAKgjp33tvZ75wBE2mnzx+++eQpAhI23/v/b3xoDEF1b93/9WwDEVu/h778HQGQngPWHv//1MQBR9X56Ad4DEFX70wuwD0BUtacXoA1AVL2bXoBNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACDYzjvt5tp8a7HWbHfOAahTN71W2gVp9W4BqEtXm1mWpH0FQD0aN7OtSWsMQB263si6KBvXAFS/f3azr8ouANXvNM+ynAJQ9W5beZaldQtAzAeAFIcAAAKtk29dOgBUvHa+dWkDUPGa+dalCUC1u1nLty5rAFS8RS0MAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAkEyOhsOjCQCRAjg56H7v4ASACAFMht2fDScARAdg0L3XAIDYABx1H3QEQFwAJgcPARxMAIgKwKg71QiAqAAMpwEMAYgKwGAawACAqAAcTgP4AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACwwIVZ1AsqAQgUwEa+j9kEoOIA9vN9zD4AFQewqNfU1w7AZDToH3Tn6qA/GE1CBXCT6xzQuokTwN1Rv5uy/tFdkACST3k+5VMSJYDLD90MDS6DBJDsZv+Q3SRKAJN+N1P9SZAArjKfBDauowRwedjN2OFliACS8Xq2j1gfJzECuBt2Mze8CxFAcrWZ5RPaV0mUAE66OToJEkBy02ulvv7v3SZRArjr5wHQvwsSwNfOO+3m2nw/eq3Z7pyXfRveqOQBIMUhoOQhzbtg9nxDAzDKB2AEQMUBDPIBGABQcQD9fAD6AFQbwF03ZwBU/AgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAlABKfp8fAGUDKPl9fgCUDaDk9/kBUDaAkt/nB0DZAG7zvc8v7+s8tqZ/4hYAiwVQ8vv8OtM/sQPAggGU+z6/T4WTAiAtgKt/Z13/At7ndzV1G9q8AmDRAMp9n9+nsA8AUQAo931+WyFfAkYCoNT3+d0//pT6jtCYASRlvs/v9v3/Pnft/T8JAGUBKHODZtzrbG11euH9+QMQ7g4dAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQA0AHORb/z4AFQfQzwfgAwAVBzDKB2AEQMUBnOQDcAJAxQHc5ToH9O8AqDiAfIeAvAcAAMoHkAyzr/8wAaD6AC4Ps67/4SUANQCQTDLuBRxMEgDqACC5/JBl/QeXCQD1AJDcHaW+F+gf3SUA1AXAt/PAaNCf81Rw0B+MJgV9LAChAJjqsOg9XwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIBoAwT/HH4CXBRD8c/wBeFkAwT/HH4CXBRD8c/wBeFkAwT/HH4AXBhD6c/wBeGkAgT/HH4CXBhD4c/wBeHEA35/j3w71Of4ALACAAAAAAAAAAAAAAAAAAAAAym7wywNhrFFUAEZFPxNW1QJwVPgjIVUpAJdTzws6uLRGUQGYfoKsA0BsAB4+QXZohaIDcP/5oQcTKxQdgOTu553A6D8WKEIAXw8CR6PhcHTkzz9WAAJAAAgAASAABIAAEAACQAAIAAEgAASAABAAAkAACAABIAAEgAAQAAJAAAgAASAABIAAEAACQAAIAAEAgAAQAAJAAAgAASAABIAAEAACQAAIAAEgAASAAFB9+i/yalmq1SK3QwAAAABJRU5ErkJggg=="


def write_app_files():
    for size, b64 in ((192, ICON_192), (512, ICON_512)):
        with open(os.path.join(OUT, f"icon-{size}.png"), "wb") as f:
            f.write(base64.b64decode(b64))
    manifest = {"name": "Chartviewer", "short_name": "Chartviewer", "description": "Market research terminal for India, US and global markets",
                "start_url": "../", "scope": "../", "display": "standalone", "background_color": "#0e141a", "theme_color": "#16233f",
                "icons": [{"src": "icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable"},
                          {"src": "icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"}]}
    with open(os.path.join(OUT, "manifest.webmanifest"), "w") as f:
        json.dump(manifest, f)


def now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def main():
    os.makedirs(OUT, exist_ok=True)
    mode = (sys.argv[1] if len(sys.argv) > 1 else "all").lower()
    prev = previous_meta()
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    meta = {"generatedAt": now_iso(), "markets": {}}
    try:
        nse_pairs = nse_symbols()
    except Exception as e:
        print(f"Couldn't read the NSE lists ({e}); using the built-in Nifty 200")
        nse_pairs = [(x, x + ".NS") for x in NIFTY200_BUILTIN]
    us_pairs = us_symbols()
    for market, key, syms in (("NSE", "nse", nse_pairs), ("US", "us", us_pairs), ("GLB", "glb", global_pairs())):
        try:
            pm = prev.get(market, {})
            hist, recent = f"{key}_history.csv", f"{key}_recent.csv"
            period = GLOBAL_PERIOD if market == "GLB" else PERIOD
            dl = (lambda per, iv="1d": download_global(per, iv)) if market == "GLB" else (lambda per, iv="1d", s=syms: download(s, per, iv))
            refresh = mode in ("all", key) or (mode == "auto" and (market == "GLB" or market_open_now(market) or not pm))
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
                df, n = dl("5d")
                if df is not None and n >= 0.5 * len(syms):
                    df.to_csv(os.path.join(OUT, recent), index=False)
                    meta["markets"][market] = dict(pm, recent=recent, updatedAt=now_iso())
                    print(f"{market}: recent prices saved for {n} symbols")
                    continue
                print(f"{market}: quick refresh failed, trying a full download")
            print(f"{market}: full download of {len(syms)} tickers ({period})…")
            df, n = dl(period)
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
        except Exception as e:
            print(f"{market}: update failed ({e}); keeping previous data")
            try:
                pm = prev.get(market, {})
                if pm and copy_previous(pm.get("history") or pm.get("file")):
                    copy_previous(pm.get("recent"))
                    meta["markets"][market] = pm
            except Exception:
                pass
    if not meta["markets"]:
        sys.exit("No market data available. Check the log above.")
    meta["global"] = [{"sym": g[0], "name": g[2], "cat": g[3], "exch": g[4]} for g in GLOBAL]
    extras = {}
    for name, fn in (("intraday", lambda: intraday_update(mode, nse_pairs, us_pairs)),
                     ("long", lambda: long_update(mode, nse_pairs, us_pairs))):
        try:
            extras[name] = fn()
        except Exception as e:
            print(f"{name} update failed: {e}")
            extras[name] = (previous_meta_full().get("extras") or {}).get(name, {})
    meta["extras"] = extras
    with open(os.path.join(OUT, "meta.json"), "w") as f:
        json.dump(meta, f, indent=1)
    for name, fn in (("EOD", lambda: eod_update({p[0] for p in nse_pairs})),
                     ("quality", lambda: quality_check(nse_pairs)), ("app files", write_app_files),
                     ("fundamentals", lambda: fundamentals_update(nse_pairs, us_pairs, FUND_PER_RUN * (4 if mode == "all" else 1))),
                     ("NSE feeds", nse_feeds), ("news", news_update)):
        try:
            fn()
        except Exception as e:
            print(f"{name} update failed: {e}")
    try:
        check_alerts()
    except Exception as e:
        print(f"Alerts check failed: {e}")


if __name__ == "__main__":
    main()
