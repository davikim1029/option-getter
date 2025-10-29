
import queue
import time
import threading
import enum
from dataclasses import dataclass
from typing import Optional, Any
import requests
import uuid
from typing import Callable


class HttpMethod(enum.Enum):
    GET = "GET"
    PUT = "PUT"
    POST = "POST"
    DELETE = "DELETE"


@dataclass
class HttpResult:
    ok: bool
    status_code: Optional[int] = None
    data: Optional[Any] = None
    error: Optional[str] = None
    response: Optional[requests.Response] = None


class ApiWorker:
    def __init__(self, consumer,stop_event, min_interval: float = 1.0, default_timeout: float = 30.0, num_workers: int = 8):
        self.consumer = consumer
        self._queue = queue.Queue()
        self._min_interval = min_interval
        self._default_timeout = default_timeout
        self._stop_event = stop_event if stop_event is not None else threading.Event()

        
        
        # shared rate limiter
        self._last_call = 0.0
        self._rate_lock = threading.Lock()

        # worker pool
        self._threads = []
        for i in range(num_workers):
            t = threading.Thread(target=self._worker, name=f"ApiWorker-{i}", kwargs={"stop_event":stop_event}, daemon=True)
            t.start()
            self._threads.append(t)

    def _respect_rate_limit(self):
        with self._rate_lock:
            now = time.time()
            elapsed = now - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_call = time.time()


    def call_api(self, method: HttpMethod, url: str, timeout: float = 30.0, **kwargs) -> HttpResult:
        """
        Queue an API call and wait for result, but return early if stopped or timeout.
        """
        result = []
        self._queue.put((method, url, kwargs, result))

        start_time = time.time()
        while not result:
            if self._stop_event.is_set():
                error="ApiWorker stopped before request completed"
                status_code=500
                return HttpResult(ok=False,status_code=status_code, error=error,response={"ok":False, "data":None,"status_code":status_code,"error":error})
            if timeout is not None and (time.time() - start_time) > timeout:
                error = f"Timeout waiting for ApiWorker while calling {url}"
                status_code=408
                return HttpResult(ok=False, status_code=status_code, error=error, response={"ok":False,"data":None, "status_code":status_code,"error":error})
            time.sleep(0.01)

        return result[0]

    def stop(self):
        self._stop_event.set()
        # drain queue before joining
        for t in self._threads:
            t.join(timeout=1)


    def call_api_async(
        self,
        method: HttpMethod,
        url: str,
        callback: Optional[Callable[[HttpResult], None]] = None,
        **kwargs
    ) -> str:
        """
        Fire-and-forget API call. 
        Optionally pass a callback to process the HttpResult when ready.
        Returns a job_id you can track.
        """
        job_id = str(uuid.uuid4())
        self._queue.put((method, url, kwargs, callback, job_id))
        return job_id
    

    def _worker(self,stop_event):
        # prefer stop_event arg for compatibility, otherwise use instance stop_event
        """
        Worker loop that handles both sync and async API calls.
        Sync call_api pushes (method, url, kwargs, result)
        Async call_api_async pushes (method, url, kwargs, callback, job_id)
        """
    
        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                item = self._queue.get(timeout=1)
            except queue.Empty:
                continue
    
            # Detect format: sync vs async
            if len(item) == 5:
                method, url, kwargs, callback, job_id = item
                is_async = True
            elif len(item) == 4:
                method, url, kwargs, result = item
                callback = None
                job_id = None
                is_async = False
            else:
                self.consumer.logger.logMessage(f"[ApiWorker] Unexpected queue item format: {item}")
                self._queue.task_done()
                continue
    
            now = time.time()
            elapsed = now - self._last_call
            if elapsed < self._min_interval:
                self._stop_event.wait(self._min_interval - elapsed)
            self._last_call = time.time()
    
            try:
                timeout = kwargs.pop("timeout", self._default_timeout)
                r = None
                if method == HttpMethod.GET:
                    r = self.consumer.session.get(url, timeout=timeout, **kwargs)
                elif method == HttpMethod.PUT:
                    r = self.consumer.session.put(url, timeout=timeout, **kwargs)
                elif method == HttpMethod.POST:
                    r = self.consumer.session.post(url, timeout=timeout, **kwargs)
                elif method == HttpMethod.DELETE:
                    r = self.consumer.session.delete(url, timeout=timeout, **kwargs)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
    
                r.raise_for_status()
                result_obj = HttpResult(ok=True, status_code=r.status_code, response=r)
    
                if is_async and callback:
                    try:
                        callback(result_obj, job_id)
                    except Exception as e:
                        self.consumer.logger.logMessage(f"[ApiWorker] Async callback error: {e}")
                elif not is_async:
                    result.append(result_obj)
    
            except requests.exceptions.HTTPError as e:
                err_obj = HttpResult(
                    ok=False,
                    status_code=e.response.status_code if e.response else None,
                    error=f"HTTPError: {str(e)}",
                    response=e.response
                )
                if is_async and callback:
                    try:
                        callback(err_obj, job_id)
                    except Exception as e:
                        self.consumer.logger.logMessage(f"[ApiWorker] Async callback error: {e}")
                elif not is_async:
                    result.append(err_obj)
            except Exception as e:
                err_obj = HttpResult(ok=False, error=f"Error: {str(e)}", status_code=500,response={"ok":False,"status_code":500,"error":str(e),"data":None})
                if is_async and callback:
                    try:
                        callback(err_obj, job_id)
                    except Exception as ex:
                        self.consumer.logger.logMessage(f"[ApiWorker] Async callback error: {ex}")
                elif not is_async:
                    result.append(err_obj)
            finally:
                self._queue.task_done()
    

# ---------------------------
# Module-level singleton
# ---------------------------
_worker_instance: Optional[ApiWorker] = None

def init_worker(consumer,stop_event, min_interval: float = 1.0):
    """Initialize the module-level ApiWorker singleton."""
    global _worker_instance
    _worker_instance = ApiWorker(consumer=consumer,stop_event=stop_event, min_interval=min_interval)

def get_worker() -> ApiWorker:
    """Get the module-level ApiWorker singleton."""
    if _worker_instance is None:
        raise RuntimeError("ApiWorker not initialized. Call init_worker() first.")
    return _worker_instance
