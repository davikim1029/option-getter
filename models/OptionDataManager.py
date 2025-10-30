import threading
import time
from pathlib import Path
from services.core.file_manager import FileManager
from services.logging.logger_singleton import getLogger


class OptionDataManager:
    """
    Manages option data records using a buffered FileManager.
    Periodically rotates/sends bundles while continuing to accept new entries.
    """

    def __init__(self, filepath: str = "data/option_data/option_data.json",
                 flush_interval: int = 600, max_buffer_size: int = 10000,
                 bundle_interval: int = 1800, bundle_limit: int = 250,
                 stop_event = None):

        self.filepath = Path(filepath)
        self.logger = getLogger()
        self.file_manager = FileManager(
            filepath=self.filepath,
            flush_interval=flush_interval,
            max_buffer_size=max_buffer_size,
            stop_event=stop_event
        )

        self.bundle_interval = bundle_interval
        self.bundle_limit = bundle_limit
        self._stop_bundler = threading.Event()
        self._bundler_thread = threading.Thread(target=self._bundle_loop, daemon=True)
        self._bundler_thread.start()

        self.logger.logMessage(f"[OptionDataManager] Started bundler thread every {bundle_interval}s")

    def add_option_record(self, record: dict):
        """Queue an option record for writing."""
        self.file_manager.add_entry(record)

    def _bundle_loop(self):
        """Background thread to periodically combine and upload temp bundles."""
        while not self._stop_bundler.is_set():
            time.sleep(self.bundle_interval)
            try:
                self.logger.logMessage("[OptionDataManager] Triggering periodic bundle rotation...")
                self.file_manager.combine_and_rotate(bundle_limit=self.bundle_limit)
            except Exception as e:
                self.logger.logMessage(f"[OptionDataManager] Bundle rotation error: {e}")

    def close(self, combine_temp: bool = True):
        """Flush, stop threads, and optionally send remaining files."""
        self._stop_bundler.set()
        self.file_manager.close()
        if combine_temp:
            self.file_manager.combine_temp_files()
        self.logger.logMessage("[OptionDataManager] Closed cleanly.")
