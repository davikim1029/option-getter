# scanner_utils.py

import yfinance as yf
import time as pyTime
import os
import json
from shared_options.models.tickers import fetch_us_tickers_from_finnhub
from services.core.cache_manager import TickerCache,RateLimitCache
from datetime import datetime, timedelta, time,timezone
import pandas_market_calendars as mcal
import pytz


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
        