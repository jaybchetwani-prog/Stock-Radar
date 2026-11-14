# Stock radar

Scans the Nifty 200 and your US tickers for morning star, hammer, near-breakout and support-bounce setups on daily, weekly and monthly candles.

Prices are fetched from Yahoo Finance by GitHub Actions every 15 minutes during market hours and published with GitHub Pages. Open the site on any device; it refreshes on its own.

- Change US stocks: edit `us_tickers.txt`.
- Update the Nifty 200 list: replace `ind_nifty200list.csv` (NSE rebalances in March and September).
- Refresh now: Actions tab → "Update stock radar" → Run workflow.
