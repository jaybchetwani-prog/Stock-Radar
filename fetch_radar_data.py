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


def download(pairs, period):
    frames, missing = [], []
    for start in range(0, len(pairs), 50):
        batch = [p[0] for p in pairs[start:start + 50]]
        tickers = [p[1] for p in pairs[start:start + 50]]
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
    latest = {}
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
        if key in fired or (mk, sym) not in latest:
            continue
        day, close = latest[(mk, sym)]
        if (cond == "above" and close >= price) or (cond == "below" and close <= price):
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


def now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def main():
    os.makedirs(OUT, exist_ok=True)
    mode = (sys.argv[1] if len(sys.argv) > 1 else "all").lower()
    prev = previous_meta()
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    meta = {"generatedAt": now_iso(), "markets": {}}
    for market, key, syms in (("NSE", "nse", nse_symbols()), ("US", "us", us_symbols())):
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
            df, n = download(syms, "5d")
            if df is not None and n >= 0.5 * len(syms):
                df.to_csv(os.path.join(OUT, recent), index=False)
                meta["markets"][market] = dict(pm, recent=recent, updatedAt=now_iso())
                print(f"{market}: recent prices saved for {n} symbols")
                continue
            print(f"{market}: quick refresh failed, trying a full download")
        print(f"{market}: full download of {len(syms)} tickers ({PERIOD})…")
        df, n = download(syms, PERIOD)
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
    try:
        eod_update({p[0] for p in nse_symbols()})
    except Exception as e:
        print(f"EOD update failed: {e}")
    check_alerts()


if __name__ == "__main__":
    main()
