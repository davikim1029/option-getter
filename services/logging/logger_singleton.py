from services.logging.logger import Logger
from services.core.shutdown_handler import ShutdownManager

_logger = None
_registered = False

def getLogger():
    global _logger, _registered
    if _logger is None:
        _logger = Logger()
    if not _registered:
        ShutdownManager.register("Singleton Logger", lambda reason=None: _logger._log_exit())
        _registered = True
    return _logger
