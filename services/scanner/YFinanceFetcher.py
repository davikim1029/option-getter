import time
import yfinance as yf
from datetime import datetime, timedelta
from services.core.cache_manager import RateLimitCache
from shared_options.log.logger_singleton import getLogger
from services.scanner.scanner_utils import is_rate_limited, wait_rate_limit

class YFTooManyAttempts(Exception):
    """Raised when too many YFinance attempts"""
    pass

class YFinanceFetcher:
    def __init__(self, rate_cache: RateLimitCache, default_cooldown_seconds=60):
        self.rate_cache = rate_cache
        self.default_cooldown = default_cooldown_seconds  # fallback cooldown
        self.logger = getLogger()

    def _update_cooldown(self, wait_seconds: int):
        """Set a cooldown for YFinance overall."""
        self.rate_cache.add("YFinance", wait_seconds)

    def fetch_ticker(self, ticker: str, max_retries=5) -> dict:
        """Fetch a yfinance Ticker object with caching and rate-limit handling."""
        retries = 0
        while retries < max_retries:
            if is_rate_limited(self.rate_cache, "YFinance"):
                wait_rate_limit(self.rate_cache, "YFinance")

            try:
                obj = yf.Ticker(ticker)
                # Minimal call to trigger possible rate-limit errors
                info = obj.info  

                # Success: clear rate cache under lock
                with self.rate_cache._lock:
                    if "YFinance" in self.rate_cache._cache:
                        del self.rate_cache._cache["YFinance"]
                return info

            except Exception as e:
                retries += 1
                self.logger.logMessage(f"[yFinance] Error fetching {ticker}: {e}. Retry {retries}/{max_retries}")
                # Exponential backoff, capped at 30 mins
                wait_time = min(self.default_cooldown * (3 ** retries), 1800)
                self._update_cooldown(wait_time)
                time.sleep(wait_time)

        raise YFTooManyAttempts(f"Failed to fetch ticker {ticker} after {max_retries} retries.")
