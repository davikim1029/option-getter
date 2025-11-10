#option_loop.py
from datetime import datetime, timedelta, time as dt_time
from shared_options.log.logger_singleton import getLogger
from services.scanner.option_scanner import run_option_scan
from shared_options.services.utils import wait_interruptible, wait_until_market_open
from shared_options.services.alerts import send_alert
from shared_options.services.token_status import TokenStatus
from shared_options.services.etrade_consumer import TokenExpiredError


# Get the system’s local timezone
local_tz = datetime.now().astimezone().tzinfo

# Default values for initial load; will be overridden by kwargs if present
DEFAULT_COOLDOWN_SECONDS = 60  # 60 seconds, give the scanner enoung time to drop and reset caches

token_status = TokenStatus()

_running = False 
def option_loop(**kwargs):
    stop_event = kwargs.get("stop_event")
    consumer = kwargs.get("consumer")
    caches = kwargs.get("caches")
    logger = getLogger()
    
    global _running
    if _running:
        logger.logMessage("[Option Loop] option_loop already running, skipping")  
        
    _running = True  
    
    try:
        logger.logMessage("[Option Loop] Module loaded/reloaded")

        while not stop_event.is_set():
            # Read dynamic values from kwargs

            cooldown   = kwargs.get("cooldown_seconds") or DEFAULT_COOLDOWN_SECONDS
            force_first_run = kwargs.get("force_first_run") or False

            now = datetime.now().time()
            if wait_until_market_open(stop_event) or force_first_run:                
                try:
                    run_option_scan(stop_event=stop_event, consumer=consumer, caches=caches)
                except TokenExpiredError:
                    logger.logMessage("[Option Loop] Token expired, pausing scanner.")
                    send_alert("E*TRADE token expired. Please re-authenticate.")
                    token_status.wait_until_valid(check_interval=30)
                    consumer.load_tokens(generate_new_token=False)
                    logger.logMessage("[Option Loop] Token restored, resuming scan.")
                except Exception as e:
                    logger.logMessage(f"[Option Loop Error] {e}")

                # Reset force_first_run after first execution
                kwargs["force_first_run"] = False
                logger.logMessage("[Option Loop] Waiting")
                wait_interruptible(stop_event, cooldown)
                logger.logMessage("[Option Loop] Wait interrupted")

        logger.logMessage("Option Loop interrupted. Exiting")
    finally:
        _running = False
