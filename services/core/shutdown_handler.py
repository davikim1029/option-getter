# services/core/shutdown_handler.py
import threading
from typing import Callable, List

class ShutdownManager:
    _stop_event: threading.Event = None
    _error_logger: Callable = None
    _initialized: bool = False
    _callbacks: List[Callable[[str], None]] = []

    @classmethod
    def init(cls, error_logger=None, stop_event=None):
        if cls._initialized:
            cls.log("[ShutdownManager] Already initialized.")
            return

        cls._stop_event = stop_event or threading.Event()
        cls._error_logger = error_logger
        cls._callbacks = []
        cls._initialized = True
        cls.log(f"[ShutdownManager] Initialized. Stop event set: {cls._stop_event.is_set()}")

    @classmethod
    def register(cls,name:str, callback: Callable[[str], None]):
        """
        Register a callback to be called when stop_all() is triggered.
        Callback must accept a single string argument: reason
        """
        if not callable(callback):
            raise ValueError("callback must be callable")
        cls._callbacks.append(callback)
        cls.log(f"[ShutdownManager] Callback registered: {name}")

    @classmethod
    def stop_all(cls, reason="Manual shutdown"):
        if cls._stop_event:
            cls._stop_event.set()
        cls.log(f"[ShutdownManager] stop_all triggered: {reason}")

        # Execute registered callbacks
        for cb in cls._callbacks:
            try:
                cb(reason)
            except Exception as e:
                cls.log(f"[ShutdownManager] Callback error: {e}")

    @classmethod
    def reset(cls):
        cls._stop_event = None
        cls._error_logger = None
        cls._callbacks = []
        cls._initialized = False
        cls.log("[ShutdownManager] Reset complete.")

    @classmethod
    def log(cls, msg):
        if cls._error_logger:
            try:
                cls._error_logger(msg)  # calls logger.logMessage(msg)
            except Exception:
                print(f"[ShutdownManager-log-error] Failed logging msg: {msg}")
        else:
            print(msg)

    @classmethod
    def stop_event(cls):
        return cls._stop_event

    @classmethod
    def wait_for_stop(cls, timeout=None):
        if cls._stop_event:
            cls._stop_event.wait(timeout)
