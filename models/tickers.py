import os
import requests
import json
from datetime import datetime
from services.core.cache_manager import TickerCache
from services.logging.logger_singleton import getLogger


def fetch_us_tickers_from_finnhub(ticker_cache: TickerCache):
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        raise Exception("FINNHUB_API_KEY not set in environment")

    logger = getLogger()
    logger.logMessage("[Tickers] Fetching from Finnhub...")

    url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={api_key}"
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception(f"Finnhub failed: {r.status_code} - {r.text}")

    raw_data = r.json()

    # Build ticker -> company name dictionary
    tickers_dict = {
        s["symbol"]: s.get("description", "")
        for s in raw_data
        if "." not in s["symbol"] and s.get("type") in ["Common Stock", "ADR"]
    }

    if ticker_cache is not None:
        for symbol, name in tickers_dict.items():
            ticker_cache.add(symbol, name)  # each ticker is its own key
        ticker_cache._save_cache()

    return tickers_dict
