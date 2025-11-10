import json
import threading
from pathlib import Path
from services.core.shutdown_handler import ShutdownManager
from shared_options.log.logger_singleton import getLogger
from shared_options.services.utils import try_send
from datetime import datetime,timezone


class FileManager:
    """
    Thread-safe file manager that buffers entries into temp JSONL files.
    Periodically flushes to disk and can safely combine completed temp files
    into bundles for upload or archival.
    """

    def __init__(self, filepath: str, flush_interval: int = 10, max_buffer_size: int = 100,stop_event = None):
        self.filepath = Path(filepath)
        self.temp_dir = self.filepath.parent / f"{self.filepath.stem}_tmp"
        self.flush_interval = flush_interval
        self.max_buffer_size = max_buffer_size
        self._buffer = []
        self._lock = threading.RLock()
        if stop_event is None:
            self._stop_event = threading.Event()
        else:
            self._stop_event = stop_event
        self.logger = getLogger()

        # Ensure directories exist
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        # Continue numbering from existing files
        existing_files = list(self.temp_dir.glob("*.jsonl"))
        if existing_files:
            max_index = max(int(f.stem) for f in existing_files if f.stem.isdigit())
            self._temp_file_counter = max_index
        else:
            self._temp_file_counter = 0

        # Initialize main file if missing
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

        self.logger.logMessage(f"[FileManager] Closing {self.filepath}...")
        self._flush()
        self._thread.join(timeout=3)
        self.logger.logMessage(f"[FileManager] Closed cleanly.")

    def combine_temp_files(self):
        """Combine *all* temp files into a single JSON array and delete them."""
        temp_files = sorted(self.temp_dir.glob("*.jsonl"))
        if not temp_files:
            return
        combined_path = self._combine_files(temp_files, delete=True)
        try_send(combined_path)
        self.logger.logMessage(f"[FileManager] Combined and sent all temp files: {combined_path}")

    def combine_and_rotate(self, bundle_limit: int = 100):
        """
        Combine a batch of completed temp files into a JSON bundle for upload.
        Removes them after sending. Does not block ongoing writes.
        """
        with self._lock:
            temp_files = sorted(self.temp_dir.glob("*.jsonl"))
            if not temp_files:
                return
            bundle_files = temp_files[:bundle_limit]

        combined_path = self._combine_files(bundle_files, delete=True)
        if combined_path:
            self.logger.logMessage(f"[FileManager] Created bundle: {combined_path}")
            try:
                try_send(combined_path)
                self.logger.logMessage(f"[FileManager] Sent bundle: {combined_path}")
            except Exception as e:
                self.logger.logMessage(f"[FileManager] Upload failed for {combined_path}: {e}")

    # ----------------------------
    # Internal Helpers
    # ----------------------------
    def _combine_files(self, file_list, delete=False):
        """Helper: combine multiple .jsonl files into one JSON array bundle."""
        if not file_list:
            return None

        all_entries = []
        for temp_file in file_list:
            try:
                with open(temp_file, "r", encoding="utf-8") as f:
                    for line in f:
                        all_entries.append(json.loads(line))
                if delete:
                    temp_file.unlink()
            except Exception as e:
                self.logger.logMessage(f"[FileManager] Error reading {temp_file}: {e}")

        if not all_entries:
            return None
        timestamp=datetime.now().astimezone().isoformat()
        bundle_path = self.filepath.parent / f"{self.filepath.stem}_bundle_{timestamp}.json"
        with open(bundle_path, "w", encoding="utf-8") as f:
            json.dump(all_entries, f, indent=2)

        return bundle_path

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
        except Exception as e:
            self.logger.logMessage(f"[FileManager] Flush error: {e}")
                
    def _write_temp_file(self, entries):
        """Write a batch of entries to a new temp JSONL file, safely serializing datetimes."""
        self._temp_file_counter += 1
        temp_path = self.temp_dir / f"{self._temp_file_counter:06d}.jsonl"

        with open(temp_path, "w", encoding="utf-8") as f:
            for entry in entries:
                # If entry is a Pydantic model, use its built-in JSON serialization
                if hasattr(entry, "json"):
                    f.write(entry.json(separators=(",", ":")) + "\n")
                else:
                    # For dicts or objects, serialize with default=str to handle datetime
                    f.write(json.dumps(entry, separators=(",", ":"), default=str) + "\n")

