# scanner_utils.py

import yfinance as yf
import time as pyTime
import os
import json
from models.option import OptionContract,OptionGreeks
from models.tickers import fetch_us_tickers_from_finnhub
from services.core.cache_manager import TickerCache,RateLimitCache
from datetime import datetime, timedelta, time,timezone
import requests
from services.logging.logger_singleton import getLogger
from pathlib import Path



def wait_interruptible(stop_event, seconds):
    """Sleep in small chunks so stop_event can interrupt immediately."""
    end_time = pyTime.time() + seconds
    while pyTime.time() < end_time and not stop_event.is_set():
        pyTime.sleep(0.5)



################################ TICKER CACHE ####################################

def get_active_tickers(ticker_cache:TickerCache = None):
    if ticker_cache is not None:
        ticker_cache._load_cache()
        if ticker_cache.is_empty():
            tickers = fetch_us_tickers_from_finnhub(ticker_cache=ticker_cache)
        else:
            tickers = ticker_cache._cache.keys()
    else:
        tickers = fetch_us_tickers_from_finnhub(ticker_cache=ticker_cache)
    return tickers

def get_next_run_date(seconds_to_wait: int) -> str:
    """
    Returns the next run time as a string in 12-hour format (AM/PM),
    adding seconds_to_wait to the current time while rolling over AM/PM half-days.
    Fully timezone-aware.
    """
    HALF_DAY = 12 * 60 * 60  # 43,200 seconds

    now = datetime.now().astimezone()  # aware datetime
    tz = now.tzinfo  # preserve timezone info

    # Seconds into current 12-hour half (0–43,199)
    seconds_in_half = (now.hour % 12) * 3600 + now.minute * 60 + now.second

    # Total seconds after wait
    total_seconds = seconds_in_half + seconds_to_wait

    # How many half-days to roll over, remainder seconds
    carry_halves, rem_seconds = divmod(total_seconds, HALF_DAY)

    # Determine current AM/PM half: 0 = AM, 1 = PM
    current_half = 0 if now.hour < 12 else 1
    new_half = (current_half + carry_halves) % 2

    # Anchor base datetime at midnight (AM) or noon (PM) in same tz
    base_time = time(0, 0) if new_half == 0 else time(12, 0)
    base = datetime.combine(now.date(), base_time, tzinfo=tz)

    # Add remaining seconds
    next_run = base + timedelta(seconds=rem_seconds)

    return next_run.strftime("%I:%M %p")


from datetime import datetime, timedelta

def is_rate_limited(cache: RateLimitCache, key: str) -> bool:
    """
    Check if a rate limit for `key` is still active.
    Returns True if still limited, False if expired.
    """
    item = cache._cache.get(key)
    if not item:
        return False  # not cached at all

    reset_seconds = item.get("Value")
    timestamp = item.get("Timestamp")

    if reset_seconds is None or timestamp is None:
        return False  # malformed entry, treat as expired

    reset_time = timestamp + timedelta(seconds=reset_seconds)
    if datetime.now().astimezone() >= reset_time:
        # expired, remove from cache
        with cache._lock:
            del cache._cache[key]
        return False

    return True



def wait_rate_limit(cache: RateLimitCache, key: str):
    """
    If the rate limit for `key` is still active, wait the remaining time.
    Removes the cache entry if expired.
    """
    item = cache._cache.get(key)
    if not item:
        return  # no limit, proceed

    reset_seconds = item.get("Value")
    timestamp = item.get("Timestamp")

    if reset_seconds is None or timestamp is None:
        return  # malformed entry, treat as expired

    # Ensure timestamp is a datetime object
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)

    reset_time = timestamp + timedelta(seconds=reset_seconds)
    now = datetime.now().astimezone()

    if now >= reset_time:
        # expired, remove from cache
        with cache._lock:
            del cache._cache[key]
        return

    # Calculate remaining wait time in seconds
    remaining = (reset_time - now).total_seconds()
    print(f"[RateLimit] Waiting {remaining:.1f} seconds for {key}...")
    pyTime.sleep(remaining)

    # Once slept, remove entry
    with cache._lock:
        cache._cache.pop(key, None)


from typing import Optional
from datetime import datetime
from shared_options.models import OptionFeature  # your shared class

def option_contract_to_feature(opt: OptionContract) -> OptionFeature:
    """
    Convert an OptionContract instance into a shared OptionFeature Pydantic model.
    """
    # Compute days to expiration
    days_to_exp = None
    if opt.expiryDate:
        days_to_exp = (opt.expiryDate.astimezone() - datetime.now().astimezone()).total_seconds() / 86400.0

    # Spread and mid price
    spread = None
    mid_price = None
    if opt.bid is not None and opt.ask is not None:
        spread = float(opt.ask) - float(opt.bid)
        mid_price = (float(opt.ask) + float(opt.bid)) / 2.0

    # Moneyness
    moneyness = None
    if opt.nearPrice is not None and opt.strikePrice is not None:
        moneyness = (float(opt.nearPrice) - float(opt.strikePrice)) / float(opt.nearPrice)

    # Greeks
    greeks = opt.OptionGreeks or OptionGreeks()

    feature = OptionFeature(
        symbol=opt.symbol,
        osiKey=opt.osiKey,
        optionType=1 if opt.optionType.upper() == "CALL" else 0,
        strikePrice=float(opt.strikePrice),
        lastPrice=float(opt.lastPrice) if opt.lastPrice is not None else 0.0,
        bid=float(opt.bid) if opt.bid is not None else 0.0,
        ask=float(opt.ask) if opt.ask is not None else 0.0,
        bidSize=float(opt.bidSize) if opt.bidSize is not None else 0.0,
        askSize=float(opt.askSize) if opt.askSize is not None else 0.0,
        volume=float(opt.volume) if opt.volume is not None else 0.0,
        openInterest=float(opt.openInterest) if opt.openInterest is not None else 0.0,
        nearPrice=float(opt.nearPrice) if opt.nearPrice is not None else 0.0,
        inTheMoney=1 if (opt.inTheMoney or "").lower().startswith("y") else 0,
        delta=float(greeks.delta) if greeks.delta is not None else 0.0,
        gamma=float(greeks.gamma) if greeks.gamma is not None else 0.0,
        theta=float(greeks.theta) if greeks.theta is not None else 0.0,
        vega=float(greeks.vega) if greeks.vega is not None else 0.0,
        rho=float(greeks.rho) if greeks.rho is not None else 0.0,
        iv=float(greeks.iv) if greeks.iv is not None else 0.0,
        daysToExpiration=float(days_to_exp) if days_to_exp is not None else 0.0,
        spread=spread,
        midPrice=mid_price,
        moneyness=moneyness,
        timestamp=datetime.now(timezone.utc)
    )

    return feature


def try_send(filepath: Path):
    logger = getLogger()
    try:
        #server_url = "http://<MACBOOK_IP>:8000/ingest"
        server_url="http://100.80.212.116:8000/api/upload_file"
        with open(filepath, "rb") as f:
            files = {"file": (filepath.name, f, "application/json")}
            resp = requests.post(server_url, files=files, timeout=900)
        if resp.status_code == 200:
            logger.logMessage(f"Sent {filepath.name} to server.")
            # Optionally delete after successful send
            filepath.unlink()
        else:
            logger.logMessage(f"[!] Server error {resp.status_code}: keeping file.")
    except Exception as e:
        logger.logMessage(f"[!] Network issue: could not send {filepath.name}. Error: {e}")
