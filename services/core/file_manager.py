# services/core/file_manager.py
import json
import os
import threading
from pathlib import Path
from services.core.shutdown_handler import ShutdownManager
from services.logging.logger_singleton import getLogger

class FileManager:
    """
    File manager that buffers entries and writes them to temporary JSONL files.
    Can accept Python objects (with `.dict()` or `__dict__`) or dicts.
    Optimized for large files and safe shutdown.
    """

    def __init__(self, filepath: str, flush_interval: int = 10, max_buffer_size: int = 100):
        self.filepath = Path(filepath)
        self.temp_dir = self.filepath.parent / f"{self.filepath.stem}_tmp"
        self.flush_interval = flush_interval
        self.max_buffer_size = max_buffer_size
        self._buffer = []
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self.logger = getLogger()
        self._temp_file_counter = 0

        # Ensure directories exist
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        # Initialize main file if empty
        if not self.filepath.exists():
            with open(self.filepath, "w", encoding="utf-8") as f:
                f.write("[]")

        # Register with ShutdownManager
        try:
            ShutdownManager.register(
                f"FileManager({self.filepath.name})",
                lambda reason=None: self.close()
            )
        except TypeError:
            ShutdownManager.init(error_logger=self.logger.logMessage)
            ShutdownManager.register(
                f"FileManager({self.filepath.name})",
                lambda reason=None: self.close()
            )

        # Start background flush thread
        self._thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._thread.start()
        self.logger.logMessage(f"[FileManager] Initialized for {self.filepath}")

    # ----------------------------
    # Public Interface
    # ----------------------------
    def add_entry(self, entry):
        """Queue a Python object or dict for later write."""
        with self._lock:
            if hasattr(entry, "dict"):
                entry = entry.dict()
            elif not isinstance(entry, dict):
                entry = entry.__dict__

            self._buffer.append(entry)
            if len(self._buffer) >= self.max_buffer_size:
                self._flush()

    def close(self):
        """Flush any remaining buffer and stop the background thread."""
        with self._lock:
            if self._stop_event.is_set():
                return
            self._stop_event.set()

        self.logger.logMessage(f"[FileManager] Flushing and closing {self.filepath}...")
        self._flush()
        self._thread.join(timeout=3)
        self.logger.logMessage(f"[FileManager] Closed cleanly.")

    def combine_temp_files(self):
        """Combine all temporary JSONL files into a single JSON array."""
        all_entries = []
        temp_files = sorted(self.temp_dir.glob("*.jsonl"))
        for temp_file in temp_files:
            with open(temp_file, "r", encoding="utf-8") as f:
                for line in f:
                    all_entries.append(json.loads(line))
            temp_file.unlink()  # remove temp file after reading

        # Write final JSON array
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(all_entries, f, indent=2)

        # Remove temp directory if empty
        try:
            self.temp_dir.rmdir()
        except OSError:
            pass

        self.logger.logMessage(f"[FileManager] Combined {len(all_entries)} entries into {self.filepath}")

    # ----------------------------
    # Internal Helpers
    # ----------------------------
    def _flush_loop(self):
        while not self._stop_event.is_set():
            self._flush()
            self._stop_event.wait(self.flush_interval)

    def _flush(self):
        with self._lock:
            if not self._buffer:
                return
            tmp_entries = self._buffer
            self._buffer = []

        try:
            self._write_temp_file(tmp_entries)
            self.logger.logMessage(
                f"[FileManager] Flushed {len(tmp_entries)} entries to temp files"
            )
        except Exception as e:
            self.logger.logMessage(f"[FileManager] Flush error: {e}")

    def _write_temp_file(self, entries):
        """Write a batch of entries to a new temp JSONL file."""
        self._temp_file_counter += 1
        temp_path = self.temp_dir / f"{self._temp_file_counter:06d}.jsonl"
        with open(temp_path, "w", encoding="utf-8") as f:
            for entry in entries:
                f.write(json.dumps(entry, separators=(",", ":")) + "\n")
