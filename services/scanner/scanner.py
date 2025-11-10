#scanner.py
import os
import sys
import queue
from pathlib import Path
import time as pyTime

from shared_options.log.logger_singleton import getLogger
from shared_options.services.etrade_consumer import EtradeConsumer
from shared_options.services.shutdown_handler import ShutdownManager
from services.core.cache_manager import Caches
import services.threading.api_worker as api_worker_mod

from services.scanner import option_loop as opt_mod
from shared_options.services.utils import get_project_root_os


from services.threading.thread_manager import ThreadManager

# ---------------------------
# Cache autosave wrapper
# ---------------------------
def _autosave_loop(stop_event, cache):
    logger = getLogger()
    try:
        cache.autosave_loop(stop_event)
    except Exception as e:
        logger.logMessage(f"[Autosave] Error in '{getattr(cache,'name',cache)}': {e}")


# ---------------------------
# Main scanner runner
# ---------------------------
def run_scan(stop_event, mode=None, consumer=None):
    logger = getLogger()
    logger.logMessage("[Scanner] Initializing...")

    if consumer is None:
        consumer = EtradeConsumer(sandbox=False)

    if (ThreadManager._caches):
        caches = ThreadManager._caches
    else: 
        caches = Caches()
    
    api_worker_mod.init_worker(consumer,stop_event=stop_event, min_interval=2)
    consumer.apiWorker = api_worker_mod.get_worker()

    manager = ThreadManager.instance(consumer=consumer)

    #parent_dir = Path(__file__).parent.resolve()
    root = Path(get_project_root_os()).resolve()
    manager.start_watcher([root])
    logger.logMessage(f"[Scanner] Watchdog started in: {root}")

    # ---------------------------
    # Shutdown callback
    # ---------------------------
    def _shutdown_callback(reason=None):
        ThreadManager.instance().stop_all()

    try:
        ShutdownManager.register("Request Shutdown", _shutdown_callback)
    except TypeError:
        ShutdownManager.init(error_logger=logger.logMessage)
        ShutdownManager.register("Request Shutdown", _shutdown_callback)

    # ---------------------------
    # Start threads
    # ---------------------------

    # Cache autosave loops
    for loop_func, loop_name in caches.all_autosave_loops():
        manager.add_thread(loop_name, loop_func, daemon=True, reload_files=[])
        
    
    #Note functions called directly by main cannot be reloaded presently since we only are able 
    # to update references to code that's being restarted through threads
    #manager.register_module("services.scanner.scanner_entry")


    # Trading loops (buy/sell)
    # NOTE: reload triggers an *immediate run* with fresh defaults from loop files

    manager.add_thread(
      "Option Scanner",
      opt_mod.option_loop,
      kwargs={
          "stop_event":stop_event,
          "consumer": consumer,
          "caches": caches,
          "start_time": getattr(opt_mod, "DEFAULT_START_TIME", None),
          "end_time": getattr(opt_mod, "DEFAULT_END_TIME", None),
          "cooldown_seconds": getattr(opt_mod, "DEFAULT_COOLDOWN_SECONDS", 300),
          "force_first_run": False,
      },
      daemon=True,
      reload_files=[
          "services/scanner/option_loop.py",
          "services/scanner/option_scanner.py",
          "services/scanner/scanner_utils.py"

      ],
      module_dependencies=[
          "services.scanner.option_scanner",
          "services.scanner.scanner_entry",
      ]
      )

    logger.logMessage("[Scanner] All threads started. Press Ctrl+C or type 'exit' to stop.")
