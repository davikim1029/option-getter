# services/scanner/option_scanner.py
import threading
import queue
from datetime import datetime, timezone
from dataclasses import dataclass
from shared_options.log.logger_singleton import getLogger
from services.scanner.scanner_utils import get_active_tickers
from shared_options.services.token_status import TokenStatus
from services.scanner.YFinanceFetcher import YFTooManyAttempts
from shared_options.services.etrade_consumer import TokenExpiredError, NoOptionsError, NoExpiryError, InvalidSymbolError
from services.core.cache_manager import (
    LastTickerCache,
    IgnoreTickerCache,
)

from shared_options.services.utils import option_contract_to_feature
from shared_options.models import OptionFeature
from shared_options.services.alerts import send_alert
from shared_options.services.utils import is_json, write_scratch, get_job_count
from shared_options.models.OptionDataManager import OptionDataManager
import json
logger = getLogger()

optionDataManager = OptionDataManager()


# ------------------------- Globals -------------------------
counter_lock = threading.Lock()
api_worker_lock = threading.Lock()
total_tickers = 0
remaining_ticker_count = 0
processed_counter = 0
processed_counter_opts = 0
total_iterated = 0

token_status = TokenStatus()
print("[Option Scanner] Module loaded/reloaded")  # hot reload indicator


def _reset_globals():
    global counter_lock, api_worker_lock
    global total_tickers, remaining_ticker_count, processed_counter, processed_counter_opts, total_iterated
    counter_lock = threading.Lock()
    api_worker_lock = threading.Lock()
    total_tickers = 0
    remaining_ticker_count = 0
    processed_counter = 0
    processed_counter_opts = 0
    total_iterated = 0


_reset_globals()


# ------------------------- Analysis logic -------------------------
def save_ticker(ticker, options, context, caches, config):
    logger = getLogger()

    last_ticker_cache = getattr(caches, "last_seen", None) or LastTickerCache()
    
    for opt in options:
        features = option_contract_to_feature(opt)
        optionDataManager.add_option_record(features)
        with counter_lock:
            global processed_counter_opts
            processed_counter_opts += 1
            if processed_counter_opts % 10000 == 0:
                logger.logMessage(
                    f"[Option Scanner] Thread {threading.current_thread().name} | Processed {processed_counter_opts} options."
                )


    with counter_lock:
        global processed_counter
        processed_counter += 1
        if processed_counter % 5 == 0 and last_ticker_cache:
            last_ticker_cache.add("lastSeen", ticker)
        if processed_counter % 250 == 0 or total_iterated == remaining_ticker_count:
            logger.logMessage(
                f"[Option Scanner] Thread {threading.current_thread().name} | Processed {processed_counter} tickers. {remaining_ticker_count - total_iterated} Remaining"
            )

    return


# ------------------------- Post-processing (stub) -------------------------
def post_process_results(results, caches, stop_event=None):
    optionDataManager.close()
    getLogger().logMessage("[Option Scanner] Running post_process_results.")


# ------------------------- Main scanner entrypoint -------------------------
def run_option_scan(stop_event, consumer=None, caches=None):
    
    global optionDataManager
    optionDataManager.stop_event = stop_event
    optionDataManager.file_manager.stop_event = stop_event
    
    
    logger = getLogger()
    logger.logMessage("[Option Scanner] Starting run_option_scan")
    _reset_globals()

    # Config
    ticker_cache = getattr(caches, "ticker", None)
    ignore_cache = getattr(caches, "ignore", None) or IgnoreTickerCache()
    last_ticker_cache = getattr(caches, "last_seen", None)

    tickers_map = get_active_tickers(ticker_cache=ticker_cache)
    ticker_keys = list(tickers_map)
    if not ticker_keys:
        logger.logMessage("[Option Scanner] No tickers to process.")
        return

    start_index = 0
    last_seen = last_ticker_cache.get("lastSeen") if last_ticker_cache else None
    if last_seen and last_seen in ticker_keys:
        start_index = ticker_keys.index(last_seen) + 1
    if start_index >= len(ticker_keys) - 1:
        start_index = 0

    remaining_tickers = ticker_keys[start_index:]
    filtered_tickers = []
    ignore_skipped = bankrupt_skipped = bought_skipped = eval_skipped = 0

    for ticker in remaining_tickers:
        if ticker.upper().endswith("Q"):   # Q Suffix means bankrupt
            bankrupt_skipped += 1
            continue
        if ignore_cache.is_cached(ticker):
            ignore_skipped += 1
            continue
        filtered_tickers.append(ticker)

    logger.logMessage(f"{bankrupt_skipped} tickers skipped due to bankruptcy")
    logger.logMessage(f"{ignore_skipped} tickers skipped based on Ignore Cache")

    global total_tickers, remaining_ticker_count
    total_tickers = remaining_ticker_count = len(filtered_tickers)

    logger.logMessage(f"[Option Scanner] {start_index} tickers processed earlier. {remaining_ticker_count} remaining.")

    if consumer is None:
        logger.logMessage("Consumer class is not instantiated, exiting...")
        return
    
    consumer.token_status.wait_until_valid()
    context = {"consumer": consumer}
    try:
        context["exposure"] = consumer.get_open_exposure()
    except TokenExpiredError:
        logger.logMessage("[Option Scanner] Token expired gathering exposure, pausing scanner.")
        send_alert("E*TRADE token expired. Please re-authenticate.")
        token_status.wait_until_valid(check_interval=30)
        consumer.load_tokens(generate_new_token=False)
        context["exposure"] = consumer.get_open_exposure()
    except Exception as e:
        logger.logMessage(f"[Option Scanner] Error getting open exposure: {e}")

    # Threading config
    scanner_cfg = getattr(caches, "scanner_config", {}) or {}
    num_api_threads = int(max(4, get_job_count()))
    num_analysis_threads = int(max(1, get_job_count()))
    api_semaphore_limit = int(scanner_cfg.get("api_semaphore", 8))

    fetch_q, result_q = queue.Queue(), queue.Queue()
    api_semaphore = threading.Semaphore(api_semaphore_limit)

    def api_worker(stop_evt, ignore_cache=None):
        global total_iterated
        logger.logMessage(f"[Option Scanner] API worker {threading.current_thread().name} started")
        while not stop_evt.is_set():
            try:
                ticker = fetch_q.get(timeout=0.5)
            except queue.Empty:
                continue
            if ticker is None:
                fetch_q.task_done()
                break
            with api_semaphore:
                try:
                    options = consumer.get_option_chain(ticker)
                    result_q.put((ticker, options))
                except TimeoutError as e:
                    fetch_q.put(ticker)
                except NoExpiryError as e:
                    total_iterated += 1
                    error = "No expiry found"
                    if hasattr(e, "args") and len(e.args) > 0:
                        e_data = e.args[0]
                        if is_json(e_data):
                            e_data = json.loads(e_data)
                            if hasattr(e_data, "Error"):
                                error = str(e_data["Error"])
                            else:
                                error = str(e_data)
                        else:
                            error = str(e_data)
                    else:
                        error = str(e)
                    if ignore_cache is not None:
                        ignore_cache.add(ticker, error)
                except InvalidSymbolError as e:
                    total_iterated += 1
                    error = "Invalid Symbol found"
                    if hasattr(e, "args") and len(e.args) > 0:
                        e_data = e.args[0]
                        if is_json(e_data):
                            e_data = json.loads(e_data)
                            if hasattr(e_data, "Error"):
                                error = str(e_data["Error"])
                            else:
                                error_obj = e_data.get("Error")
                                if error_obj is not None:
                                    code = error_obj.get("code")
                                    message = error_obj.get("message")
                                    error = f"Code {code}: {message}"
                                else:
                                    error = str(e_data)
                        else:
                            error = str(e_data)
                    else:
                        error = str(e)
                    if ignore_cache is not None:
                        ignore_cache.add(ticker, error)
                except NoOptionsError as e:
                    total_iterated += 1
                    error = "No options found"
                    if hasattr(e, "args") and len(e.args) > 0:
                        e_data = e.args[0]
                        if is_json(e_data):
                            e_data = json.loads(e_data)
                            if hasattr(e_data, "Error"):
                                error = str(e_data["Error"])
                            else:
                                error_obj = e_data.get("Error")
                                if error_obj is not None:
                                    code = error_obj.get("code")
                                    message = error_obj.get("message")
                                    error = f"Code {code}: {message}"
                                else:
                                    error = str(e_data)
                        else:
                            error = str(e_data)
                    else:
                        error = str(e)
                    if ignore_cache is not None:
                        ignore_cache.add(ticker, error)
                except TokenExpiredError as e:
                    logger.logMessage("[Option Scanner] TokenExpiredError in api_worker.")
                    send_alert("E*TRADE token expired. Please re-authenticate.")
                    token_status.wait_until_valid(check_interval=30)
                    consumer.load_tokens(generate_new_token=False)
                    fetch_q.put(ticker)
                except Exception as e:
                    logger.logMessage(f"[Option Scanner] Error fetching options for {ticker}: {e}")
                    result_q.put((ticker, None))
                finally:
                    fetch_q.task_done()
        logger.logMessage(f"[Option Scanner] API worker {threading.current_thread().name} exiting")

    def analysis_worker(stop_evt):
        global total_iterated
        logger.logMessage(f"[Option Scanner] Analysis worker {threading.current_thread().name} started")
        while not stop_evt.is_set():
            try:
                item = result_q.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is None:
                result_q.task_done()
                break
            ticker, options = item
            total_iterated += 1
            if options is not None:
                try:
                    save_ticker(ticker, options, context, caches, {})
                except Exception as e:
                    logger.logMessage(f"[Option Scanner] analyze_ticker {ticker} error: {e}")
            else:
                logger.logMessage(f"Ticker {ticker} has no options found but was not caught as an exception")
                write_scratch(f"Ticker {ticker} has no options found but was not caught as an exception")
            result_q.task_done()
        logger.logMessage(f"[Option Scanner] Analysis worker {threading.current_thread().name} exiting")

    # Start workers
    api_threads = [threading.Thread(target=api_worker, args=(stop_event, None), name=f"Buy Fetch Thread {i}", daemon=True) for i in range(num_api_threads)]
    for t in api_threads: t.start()
    analysis_threads = [threading.Thread(target=analysis_worker, args=(stop_event,), name=f"Buy Analysis Thread {i}", daemon=True) for i in range(num_analysis_threads)]
    for t in analysis_threads: t.start()

    # Feed tickers
    for t in filtered_tickers:
        fetch_q.put(t)

    # Instead of blocking forever on join, poll with stop_event
    while not stop_event.is_set():
        if fetch_q.unfinished_tasks == 0 and result_q.unfinished_tasks == 0:
            break
        try:
            # short sleep lets workers process without busy-waiting
            stop_event.wait(0.5)
        except KeyboardInterrupt:
            stop_event.set()
            break

    # If stop_event was triggered, flush queues to let workers exit
    if stop_event.is_set():
        while not fetch_q.empty():
            try:
                fetch_q.get_nowait(); fetch_q.task_done()
            except queue.Empty:
                break
        while not result_q.empty():
            try:
                result_q.get_nowait(); result_q.task_done()
            except queue.Empty:
                break

    # Stop workers gracefully
    for _ in api_threads: fetch_q.put(None)
    for _ in analysis_threads: result_q.put(None)

    for t in api_threads + analysis_threads:
        t.join(timeout=2)

    try:
        post_process_results([], caches, stop_event)
    except Exception as e:
        logger.logMessage(f"[Option Scanner] post_process_results error: {e}")

    #If we've iterated over every ticker,  clear the last_ticker cache 
    if total_iterated == remaining_ticker_count:
        try:
            last_ticker_cache.clear()
            _reset_globals()
            logger.logMessage("Last Ticker Cache cleared and globals reset")
        except Exception:
            pass
    else:
        logger.logMessage(f"Total Iterated: {total_iterated}. Remaining Ticker Count: {remaining_ticker_count}")

    logger.logMessage("[Option Scanner] Run complete")
