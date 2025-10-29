# services/core/token_status.py
import json
import os
import time
from threading import Lock
from services.logging.logger_singleton import getLogger  # assuming you already use logger

class TokenStatus:
    def __init__(self, filepath="encryption/token_status.json"):
        self.filepath = filepath
        self.lock = Lock()
        self._ensure_file_exists()
        self.logger = getLogger()

    def _ensure_file_exists(self):
        """Create the file if missing or corrupted."""
        if not os.path.exists(self.filepath):
            self.set_status(valid=True)  # assume valid at startup
        else:
            try:
                with open(self.filepath, "r") as f:
                    json.load(f)  # just try to parse
            except Exception:
                # corrupted file, overwrite clean
                self.set_status(valid=True)

    def set_status(self, valid: bool):
        """Set current token validity flag."""
        with self.lock:
            data = {"valid": valid, "last_checked": int(time.time())}
            with open(self.filepath, "w") as f:
                json.dump(data, f)

    def is_valid(self) -> bool:
        """Read token validity; auto-heal if corrupted."""
        with self.lock:
            try:
                with open(self.filepath, "r") as f:
                    data = json.load(f)
                    return data.get("valid", False)
            except Exception:
                # if corrupted, recreate as invalid
                self.set_status(valid=False)
                return False

    def wait_until_valid(self, check_interval=60):
        """
        Block until token status is valid.
        Useful for scanners that should pause until tokens are refreshed.
        """
        while not self.is_valid():
            self.logger.logMessage("[TokenStatus] Token invalid, waiting...")
            time.sleep(check_interval)
        self.logger.logMessage("[TokenStatus] Token valid, resuming work")
