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

Alerts: if alerts.csv is in the repo, each run checks it and sends new alerts by Telegram
(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) and/or email (SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS,
ALERT_EMAIL_TO, optional ALERT_EMAIL_FROM), all stored as GitHub Actions secrets.

NSE symbols scanned = Nifty 200 list + Nifty 500 list + the ETFs and indices below + extra_tickers.txt
(one NSE symbol per line, for portfolio stocks or ETFs not covered). Prices come from Yahoo Finance.
Also writes data/nse_groups.json (ETF and index names) for the page.
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
    pairs = [(x, x + ".NS") for x in syms + n500 + read_lines("extra_tickers.txt")]
    pairs += [(e[0], e[1]) for e in ETFS] + [(i[0], i[1]) for i in INDICES]
    seen, out = set(), []
    for sym, yh in pairs:
        if sym not in seen:
            seen.add(sym)
            out.append((sym, yh))
    with open(os.path.join(OUT, "nse_groups.json"), "w") as f:
        json.dump({"etf": [{"sym": e[0], "name": e[2], "tv": e[3]} for e in ETFS],
                   "index": [{"sym": i[0], "name": i[2], "tv": i[3]} for i in INDICES]}, f, indent=1)
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


def _send_telegram(text):
    token, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          data={"chat_id": chat, "text": text}, timeout=30)
        return r.ok
    except Exception as e:
        print(f"  couldn't send Telegram message: {e}")
        return False


def _email_ready():
    return bool(os.environ.get("SMTP_HOST") and os.environ.get("ALERT_EMAIL_TO"))


def _send_email(subject, lines):
    """One email with every alert of this run. Uses SMTP settings from GitHub secrets."""
    import smtplib
    import ssl
    from email.message import EmailMessage
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT") or 587)
    user, pw = os.environ.get("SMTP_USER"), os.environ.get("SMTP_PASS")
    to = os.environ["ALERT_EMAIL_TO"]
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.environ.get("ALERT_EMAIL_FROM") or user or to
    msg["To"] = to
    body = "\n\n".join(lines) + "\n\nTechnical alerts from your Stock radar, not investment advice."
    if SITE_URL:
        body += f"\nOpen the radar: {SITE_URL}"
    msg.set_content(body)
    ctx = ssl.create_default_context()
    try:
        if port == 465:
            with smtplib.SMTP_SSL(host, port, context=ctx, timeout=30) as srv:
                if user:
                    srv.login(user, pw or "")
                srv.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=30) as srv:
                srv.starttls(context=ctx)
                if user:
                    srv.login(user, pw or "")
                srv.send_message(msg)
        return True
    except Exception as e:
        print(f"  couldn't send email: {e}")
        return False


def _rsi(c, p=14):
    out = [None] * len(c)
    if len(c) <= p:
        return out
    g = l = 0.0
    for i in range(1, p + 1):
        d = c[i] - c[i - 1]
        g += max(d, 0)
        l += max(-d, 0)
    g, l = g / p, l / p
    out[p] = 100.0 if l == 0 else 100 - 100 / (1 + g / l)
    for i in range(p + 1, len(c)):
        d = c[i] - c[i - 1]
        g = (g * (p - 1) + max(d, 0)) / p
        l = (l * (p - 1) + max(-d, 0)) / p
        out[i] = 100.0 if l == 0 else 100 - 100 / (1 + g / l)
    return out


def _ema(vals, p):
    out, k, e = [None] * len(vals), 2 / (p + 1), None
    start = next((i for i, v in enumerate(vals) if v is not None), None)
    if start is None or len(vals) - start < p:
        return out
    e = sum(vals[start:start + p]) / p
    out[start + p - 1] = e
    for i in range(start + p, len(vals)):
        e = vals[i] * k + e * (1 - k)
        out[i] = e
    return out


def daily_signals(df, lookback=20, tol=3.0):
    """Signals on the latest daily candle, matching the website's scanner:
    hammer, morning star and bullish engulfing at support, breakout above the lookback high,
    support bounce, RSI crossing 70/30 and MACD crossing its signal line."""
    o, h, l, c = (df[k].astype(float).tolist() for k in ("Open", "High", "Low", "Close"))
    n = len(c)
    if n < 40:
        return []
    body = lambda i: abs(c[i] - o[i])
    rng = lambda i: h[i] - l[i]
    down = lambda j: j >= 5 and c[j] < c[j - 5]

    def support_at(first, low_val):
        end = first - 3
        if end < 4:
            return None
        cands = [min(l[max(0, first - lookback):end + 1])]
        for j in range(max(3, first - 60), first - 3):
            if all(l[j] <= l[j + d] for d in (-3, -2, -1, 1, 2, 3)):
                cands.append(l[j])
        best = None
        for lv in cands:
            if lv > 0 and abs((low_val - lv) / lv * 100) <= tol and (best is None or abs(low_val - lv) < abs(low_val - best)):
                best = lv
        return best

    i, out = n - 1, []
    r = rng(i)
    lo_w, up_w = min(o[i], c[i]) - l[i], h[i] - max(o[i], c[i])
    if r > 0 and lo_w >= 2 * body(i) and lo_w >= 0.6 * r and up_w <= 0.15 * r and down(i - 1):
        sup = support_at(i, l[i])
        if sup:
            out.append(("hammer", f"Hammer at support {sup:,.2f}"))
    a, b = i - 2, i - 1
    if a >= 11:
        avg = sum(body(k) for k in range(a - 10, a)) / 10
        ba = body(a)
        if (c[a] < o[a] and ba >= 0.8 * avg and ba >= 0.5 * rng(a) and body(b) <= 0.35 * ba
                and max(o[b], c[b]) <= c[a] + 0.25 * ba and c[i] > o[i] and c[i] >= (o[a] + c[a]) / 2 and down(a - 1)):
            sup = support_at(a, min(l[a:i + 1]))
            if sup:
                out.append(("mstar", f"Morning star at support {sup:,.2f}"))
    if (c[i - 1] < o[i - 1] and c[i] > o[i] and o[i] <= c[i - 1] and c[i] >= o[i - 1]
            and body(i) > body(i - 1) * 1.05 and down(i - 2)):
        sup = support_at(i - 1, min(l[i - 1], l[i]))
        if sup:
            out.append(("engulf", f"Bullish engulfing at support {sup:,.2f}"))
    res, res_prev = max(h[i - lookback:i]), max(h[i - 1 - lookback:i - 1])
    if c[i] > res and c[i - 1] <= res_prev:
        out.append(("breakout", f"Broke above its {lookback}-day high of {res:,.2f}"))
    sup = min(l[i - lookback + 1:i - 2])
    recent_low = min(l[i], l[i - 1], l[i - 2])
    if sup * (1 - tol / 100) <= recent_low <= sup * (1 + tol / 100) and c[i] > o[i] and c[i] > c[i - 1]:
        out.append(("support", f"Bounced from support at {sup:,.2f}"))
    rs = _rsi(c)
    if rs[i] is not None and rs[i - 1] is not None:
        if rs[i - 1] < 70 <= rs[i]:
            out.append(("rsi70", f"RSI rose above 70 ({rs[i]:.0f}), overbought zone"))
        if rs[i - 1] > 30 >= rs[i]:
            out.append(("rsi30", f"RSI fell below 30 ({rs[i]:.0f}), oversold zone"))
    e12, e26 = _ema(c, 12), _ema(c, 26)
    line = [x - y if x is not None and y is not None else None for x, y in zip(e12, e26)]
    sig = _ema(line, 9)
    if None not in (line[i], line[i - 1], sig[i], sig[i - 1]):
        h0, h1 = line[i - 1] - sig[i - 1], line[i] - sig[i]
        if h0 < 0 <= h1:
            out.append(("macd_up", "MACD crossed above its signal line (bullish)"))
        if h0 > 0 >= h1:
            out.append(("macd_dn", "MACD crossed below its signal line (bearish)"))
    return out


def check_alerts():
    """Check alerts.csv (downloaded from the website) and send each new alert once,
    by Telegram and/or email, depending on which GitHub secrets are set.
    Condition "above"/"below" = price alert. Condition "signal" = watch the stock for new signals."""
    if not os.path.exists("alerts.csv"):
        return
    tg = bool(os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"))
    em = _email_ready()
    if not tg and not em:
        print("alerts.csv found, but no Telegram or email secrets are set (see the Guide on the website)")
        return
    alerts = pd.read_csv("alerts.csv")
    alerts.columns = [c.strip().lower() for c in alerts.columns]
    frames = {}
    for key, mk in (("nse", "NSE"), ("us", "US")):
        parts = [pd.read_csv(os.path.join(OUT, f"{key}_{kind}.csv")) for kind in ("history", "recent")
                 if os.path.exists(os.path.join(OUT, f"{key}_{kind}.csv"))]
        parts = [p for p in parts if not p.empty]
        if parts:
            frames[mk] = (pd.concat(parts).drop_duplicates(["Date", "Symbol"], keep="last")
                          .sort_values(["Symbol", "Date"]))
    latest = {}
    for mk, df in frames.items():
        for _, r in df.groupby("Symbol").tail(1).iterrows():
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
    found = []   # (key, day, text)
    for _, a in alerts.iterrows():
        sym = str(a.get("symbol", "")).strip().upper()
        mk = str(a.get("market", "NSE")).strip().upper() or "NSE"
        cond = str(a.get("condition", "")).strip().lower()
        note = a.get("note")
        note = f" [{note.strip()}]" if isinstance(note, str) and note.strip() else ""
        if (mk, sym) not in latest:
            continue
        day, close = latest[(mk, sym)]
        if cond == "signal":
            df = frames[mk]
            for name, text in daily_signals(df[df["Symbol"] == sym]):
                key = f"{mk}|{sym}|signal|{name}|{day}"
                if key not in fired:
                    found.append((key, day, f"{sym}: {text}. Close {close:,.2f}{note}."))
            continue
        try:
            price = float(a.get("price"))
        except (TypeError, ValueError):
            continue
        key = f"{mk}|{sym}|{cond}|{price}"
        if key in fired:
            continue
        if (cond == "above" and close >= price) or (cond == "below" and close <= price):
            found.append((key, day, f"{sym} is {close:,.2f}, "
                                    f"{'at or above' if cond == 'above' else 'at or below'} your level {price:,.2f}{note}."))
    sent = 0
    if found:
        ok_tg = all([_send_telegram("Stock radar alert: " + t + "\nTechnical alert only, not investment advice.") for _, _, t in found]) if tg else False
        ok_em = _send_email(f"Stock radar: {len(found)} alert{'s' if len(found) > 1 else ''}", [t for _, _, t in found]) if em else False
        if ok_tg or ok_em:
            for key, day, _ in found:
                fired[key] = day
            sent = len(found)
    with open(os.path.join(OUT, "alerts_state.json"), "w") as f:
        json.dump(state, f, indent=1)
    print(f"Alerts: {len(alerts)} rules checked, {sent} sent"
          f" (Telegram {'on' if tg else 'off'}, email {'on' if em else 'off'})")


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
    check_alerts()


if __name__ == "__main__":
    main()
