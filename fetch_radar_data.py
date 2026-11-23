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
            elif cond == "golden_cross" and len(c) > 205:
                s50, s200 = c.rolling(50).mean(), c.rolling(200).mean()
                met, what = s50.iloc[-1] > s200.iloc[-1] and s50.iloc[-2] <= s200.iloc[-2], "50-day average crossed above the 200-day average"
        if met and what:
            text = f"Stock radar alert: {sym} {what}. Price {close:,.2f}.\nTechnical alert only, not investment advice."
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
            text = (f"Stock radar alert: {sym} is {close:,.2f}, "
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
ICON_192 = "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"
ICON_512 = "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"


def write_app_files():
    for size, b64 in ((192, ICON_192), (512, ICON_512)):
        with open(os.path.join(OUT, f"icon-{size}.png"), "wb") as f:
            f.write(base64.b64decode(b64))
    manifest = {"name": "Stock radar", "short_name": "Stock radar", "description": "Market research terminal for India, US and global markets",
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
