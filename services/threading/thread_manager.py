#thread_manager.py
import threading
import time
import os
import sys
import importlib
from pathlib import Path
from datetime import datetime
from services.logging.logger_singleton import getLogger
from services.helpers import snapshot_module
from services.utils import set_reload_flag
from services.core.cache_manager import Caches

class ThreadWrapper(threading.Thread):
    def __init__(self, name, target_func, kwargs=None, daemon=True,
                 start_time=None, end_time=None, cooldown_seconds=None,
                 reload_files=None, parent=None, update_vars=None,
                 module_dependencies=None,stop_event = None, token_pause_event=None):
        super().__init__(name=name, daemon=daemon)
        self._target_func = target_func
        self._kwargs = kwargs or {}
        self._start_time = start_time
        self._end_time = end_time
        self._cooldown_seconds = cooldown_seconds
        self._reload_files = [Path(f).resolve() for f in (reload_files or [])]
        self._parent = parent
        self._update_vars = update_vars or {}
        self._module_dependencies = module_dependencies or []
        self._reload_event = threading.Event()
        self._thread_lock = threading.Lock()
        self._token_pause_event = token_pause_event  # Event to sync on token refresh
        # Accept a stop_event in kwargs if provided, else fall back to token_pause_event or create one.
        if kwargs and "stop_event" in kwargs and kwargs["stop_event"] is not None:
            self._stop_event = kwargs["stop_event"]
        else:
            # If the manager passed in a stop_event it will be in kwargs due to add_thread above.
            # Only create an Event as a last resort (explicitly private thread).
            self._stop_event = threading.Event()
        self.logger = getLogger()

    def run(self):
        while not self._stop_event.is_set():
            try:
                # Wait until start_time if defined
                if self._start_time:
                    now = datetime.now().astimezone().time()
                    start_seconds = self._start_time.hour*3600 + self._start_time.minute*60 + self._start_time.second
                    now_seconds = now.hour*3600 + now.minute*60 + now.second
                    if now_seconds < start_seconds:
                        time.sleep(min(1, start_seconds - now_seconds))
                        continue

                # Pause if token expired
                if self._token_pause_event:
                    self._token_pause_event.wait()  # blocks until token is refreshed

                # Run the main function
                self._target_func(**self._kwargs)

                # Handle cooldown
                if self._cooldown_seconds:
                    for _ in range(int(self._cooldown_seconds)):
                        if self._stop_event.is_set() or self._reload_event.is_set():
                            break
                        time.sleep(1)
                else:
                    break

                # Handle end_time
                if self._end_time:
                    now = datetime.now().astimezone().time()
                    end_seconds = self._end_time.hour*3600 + self._end_time.minute*60 + self._end_time.second
                    now_seconds = now.hour*3600 + now.minute*60 + now.second
                    if now_seconds >= end_seconds:
                        break

                # Handle reload
                if self._reload_event.is_set():
                    self._reload_event.clear()
                    self.logger.logMessage(f"[{self.name}] Reloading triggered")

            except Exception as e:
                self.logger.logMessage(f"[{self.name}] crashed: {e}")
                break

    def stop(self):
        self._stop_event.set()
        self._reload_event.set()

class ThreadManager:
    _instance = None
    _lock = threading.Lock()
    TRIGGER_FILE = Path("cache/cache_reload_trigger.txt").resolve()
    _caches = Caches()

    @classmethod
    def instance(cls, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(**kwargs)
        return cls._instance

    def __init__(self, consumer=None,stop_event = None, token_pause_event=None):
        self._threads = {}
        self._registered_modules = {}
        self._consumer = consumer
        self._watch_dir = None
        self._file_timestamps = {}
        self._watcher_thread = None
        self._manager_stop_event = stop_event or threading.Event()
        self._reload_queue = set()
        self._initial_scan_done = False
        self._token_pause_event = token_pause_event
        self._stop_event = stop_event
        self.logger = getLogger()
        
        # Ensure the manual reload trigger file exists
        if not self.TRIGGER_FILE.exists():
            self.TRIGGER_FILE.touch()
        
        
    # ---------------------------
    # Module registration (non-thread)
    # ---------------------------
    def register_module(self, module_name: str):
        """Register a non-threaded module for hot reload watching."""
        if module_name not in sys.modules:
            module = importlib.import_module(module_name)
        else:
            module = sys.modules[module_name]

        path = Path(module.__file__).resolve()
        mtime = os.path.getmtime(path)

        self._registered_modules[module_name] = {
            "path": path,
            "last_mtime": mtime,
        }
        self.logger.logMessage(f"[Register] Registered {module_name} for hot reload (non-threaded)")

    # ---------------------------
    # Thread registration
    # ---------------------------
   
    
    def add_thread(self, name, target_func, kwargs=None, daemon=True,
                   start_time=None, end_time=None, cooldown_seconds=None,
                   reload_files=None, parent=None, update_vars=None,
                   module_dependencies=None):
        if kwargs is None:
            kwargs={}
        if "stop_event" not in kwargs:
            kwargs["stop_event"] = self._manager_stop_event
        wrapper = ThreadWrapper(
            name=name,
            target_func=target_func,
            kwargs=kwargs,
            daemon=daemon,
            start_time=start_time,
            end_time=end_time,
            cooldown_seconds=cooldown_seconds,
            reload_files=reload_files,
            parent=parent,
            update_vars=update_vars,
            module_dependencies=module_dependencies,
            token_pause_event=self._token_pause_event
        )
        self._threads[name] = wrapper
        wrapper.start()
        self.logger.logMessage(f"[ThreadManager] Started thread {name}")

    # ---------------------------
    # Stop threads
    # ---------------------------
    def stop_all(self):
        self._manager_stop_event.set()  # stop watcher loop first
        for t in self._threads.values():
            t.stop()
        for t in self._threads.values():
            t.join()
        if self._watcher_thread and self._watcher_thread.is_alive():
            self._watcher_thread.join()
        self.logger.logMessage("[ThreadManager] All threads stopped")

    # ---------------------------
    # File watcher (hot-reload)
    # ---------------------------
    def start_watcher(self, watch_dirs: list[Path]):
        self._watch_dirs = [Path(d).resolve() for d in watch_dirs]
        self._file_timestamps = self._scan_files()
        self._watcher_thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._watcher_thread.start()
        self._initial_scan_done = True
        self.logger.logMessage(f"[ThreadManager] Started file watcher on {self._watch_dirs}")

    def _scan_files(self):
        file_timestamps = {}
        for watch_dir in getattr(self, "_watch_dirs", []):
            for root, _, files in os.walk(watch_dir):
                if "venv" in Path(root).parts:
                    continue
                for f in files:
                    if f.endswith(".py"):
                        path = Path(root) / f
                        try:
                            file_timestamps[path.resolve()] = path.stat().st_mtime
                        except FileNotFoundError:
                            continue
                    elif f == self.TRIGGER_FILE.name:
                        path = Path(root) / f
                        try:
                            file_timestamps[path.resolve()] = path.stat().st_mtime
                        except FileNotFoundError:
                            continue
                        
        return file_timestamps
    
    def _watch_loop(self):
        while not self._manager_stop_event.is_set():
            current_files = self._scan_files()
            for path, mtime in current_files.items():
                if self._manager_stop_event.is_set():
                    break
                if path not in self._file_timestamps or self._file_timestamps[path] < mtime:
                    self._file_timestamps[path] = mtime
                    if self._initial_scan_done:
                        self._handle_file_change(path)
            time.sleep(1)

    # ---------------------------
    # Hot-reload modules in dependency order
    # ---------------------------
    def reload_modules_in_order(self, module_names, visited=None):
        visited = visited or set()
        for mod_name in module_names:
            if mod_name in visited:
                continue
            visited.add(mod_name)

            thread = next((t for t in self._threads.values()
                           if t._target_func.__module__ == mod_name), None)
            if thread and getattr(thread, "_module_dependencies", None):
                self.reload_modules_in_order(thread._module_dependencies, visited)

            if mod_name in sys.modules:
                module = sys.modules[mod_name]
                importlib.reload(module)
                print(f"[HotReload] Reloaded {mod_name}")
            else:
                module = importlib.import_module(mod_name)
                print(f"[HotReload] Imported fresh {mod_name}")

    # ---------------------------
    # Handle file changes
    # ---------------------------
    def _handle_file_change(self, filepath: Path):
        self.logger.logMessage(f"[Watcher] Detected change in {filepath}")
        filepath = filepath.resolve()

        reloaded = set()

        found = False
        for name, wrapper in list(self._threads.items()):
            if filepath not in [Path(f).resolve() for f in wrapper._reload_files]:
                continue
            found = True
            self.logger.logMessage(f"[Watcher] Reloading thread {name} due to change in {filepath}")

            # Stop the old thread
            wrapper.stop()
            wrapper.join()

            # Reload modules in dependency order
            modules_to_reload = wrapper._module_dependencies + [wrapper._target_func.__module__]
            self.reload_modules_in_order(modules_to_reload)

            # Get fresh target function
            module = sys.modules[wrapper._target_func.__module__]
            new_target = getattr(module, wrapper._target_func.__name__)

            # Prepare new kwargs (update vars & module defaults)
            new_kwargs = dict(wrapper._kwargs)
            for key, candidate_list in (
                ("start_time", ["START_TIME", "DEFAULT_START_TIME"]),
                ("end_time", ["END_TIME", "DEFAULT_END_TIME"]),
                ("cooldown_seconds", ["COOLDOWN_SECONDS", "DEFAULT_COOLDOWN_SECONDS"])
            ):
                for candidate in candidate_list:
                    if hasattr(module, candidate):
                        new_kwargs[key] = getattr(module, candidate)
                        break

            for var_name in getattr(wrapper, "_update_vars", {}):
                if hasattr(module, var_name):
                    new_kwargs[var_name] = getattr(module, var_name)

            # Recreate thread with updated target
            new_wrapper = ThreadWrapper(
                name=wrapper.name,
                target_func=new_target,
                kwargs=new_kwargs,
                daemon=wrapper.daemon,
                start_time=new_kwargs.get("start_time", wrapper._start_time),
                end_time=new_kwargs.get("end_time", wrapper._end_time),
                cooldown_seconds=new_kwargs.get("cooldown_seconds", wrapper._cooldown_seconds),
                reload_files=wrapper._reload_files,
                parent=wrapper._parent,
                update_vars=wrapper._update_vars,
                module_dependencies=wrapper._module_dependencies,
                token_pause_event=self._token_pause_event
            )
            self._threads[name] = new_wrapper
            new_wrapper.start()
            self.logger.logMessage(f"[Watcher] Reload complete for {name} → kwargs updated: {list(new_kwargs.keys())}")
            set_reload_flag()
        
        
        # --- 2. Check registered modules (new) ---
        for mod_name, meta in self._registered_modules.items():
            try:
                mtime = os.path.getmtime(meta["path"])
                if mtime != meta["last_mtime"]:
                    self.logger.logMessage(f"[Watcher] Detected change in registered module {mod_name}")
                    meta["last_mtime"] = mtime

                    # Reload module
                    if mod_name in sys.modules:
                        importlib.reload(sys.modules[mod_name])
                        self.logger.logMessage(f"[Watcher] Reloaded {mod_name}")

                    # Restart any thread whose target module or dependencies include this module
                    for name, wrapper in list(self._threads.items()):
                        if mod_name == wrapper._target_func.__module__ or mod_name in wrapper._module_dependencies:
                            self.logger.logMessage(f"[Watcher] Restarting {name} due to dependency reload {mod_name}")

                            # Stop + join old thread
                            wrapper.stop()
                            wrapper.join()

                            # Reload target + deps
                            modules_to_reload = wrapper._module_dependencies + [wrapper._target_func.__module__]
                            self.reload_modules_in_order(modules_to_reload)

                            # Get fresh target function
                            module = sys.modules[wrapper._target_func.__module__]
                            new_target = getattr(module, wrapper._target_func.__name__)

                            # Prepare kwargs
                            new_kwargs = dict(wrapper._kwargs)
                            for key, candidate_list in (
                                ("start_time", ["START_TIME", "DEFAULT_START_TIME"]),
                                ("end_time", ["END_TIME", "DEFAULT_END_TIME"]),
                                ("cooldown_seconds", ["COOLDOWN_SECONDS", "DEFAULT_COOLDOWN_SECONDS"])
                            ):
                                for candidate in candidate_list:
                                    if hasattr(module, candidate):
                                        new_kwargs[key] = getattr(module, candidate)
                                        break

                            for var_name in getattr(wrapper, "_update_vars", {}):
                                if hasattr(module, var_name):
                                    new_kwargs[var_name] = getattr(module, var_name)

                            # Recreate wrapper
                            new_wrapper = ThreadWrapper(
                                name=wrapper.name,
                                target_func=new_target,
                                kwargs=new_kwargs,
                                daemon=wrapper.daemon,
                                start_time=new_kwargs.get("start_time", wrapper._start_time),
                                end_time=new_kwargs.get("end_time", wrapper._end_time),
                                cooldown_seconds=new_kwargs.get("cooldown_seconds", wrapper._cooldown_seconds),
                                reload_files=wrapper._reload_files,
                                parent=wrapper._parent,
                                update_vars=wrapper._update_vars,
                                module_dependencies=wrapper._module_dependencies,
                                token_pause_event=self._token_pause_event
                            )
                            self._threads[name] = new_wrapper
                            new_wrapper.start()
                            set_reload_flag()
                            found=True

                            self.logger.logMessage(
                                f"[Watcher] Reload complete for {name} → kwargs updated: {list(new_kwargs.keys())}"
                            )

            except FileNotFoundError:
                continue
            
            
        # --- 3. Manual reload trigger ---

        if filepath == self.TRIGGER_FILE:
            self.logger.logMessage(f"[Watcher] Manual reload triggered by {self.TRIGGER_FILE.name}")
            
            for cache in ThreadManager._caches.all_caches():
                cache._load_cache()
            
            for name, wrapper in list(self._threads.items()):
                self.logger.logMessage(f"[Watcher] Stopping thread {name} for manual reload")
                wrapper.stop()
                wrapper.join()

                # Reload module dependencies
                modules_to_reload = wrapper._module_dependencies + [wrapper._target_func.__module__]
                self.reload_modules_in_order(modules_to_reload)

                # Recreate thread
                module = sys.modules[wrapper._target_func.__module__]
                if not hasattr(wrapper._target_func, "__self__"):  # skip bound methods
                    new_target = getattr(module, wrapper._target_func.__name__)
                else:
                    new_target = wrapper._target_func
                new_wrapper = ThreadWrapper(
                    name=wrapper.name,
                    target_func=new_target,
                    kwargs=wrapper._kwargs,
                    daemon=wrapper.daemon,
                    reload_files=wrapper._reload_files,
                    parent=wrapper._parent,
                    update_vars=wrapper._update_vars,
                    module_dependencies=wrapper._module_dependencies,
                    token_pause_event=self._token_pause_event
                )
                self._threads[name] = new_wrapper
                new_wrapper.start()
                self.logger.logMessage(f"[Watcher] Restarted thread {name} after manual trigger")

            set_reload_flag()
            found = True


        if not found:
            self.logger.logMessage(
                f"[Watcher] File {filepath.parts[-1]} is not registered → no hot reload performed"
            )

    # ---------------------------
    # Wait for shutdown
    # ---------------------------
    def wait_for_shutdown(self):
        try:
            while not self._manager_stop_event.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            self._manager_stop_event.set()
            self.logger.logMessage("[ThreadManager] KeyboardInterrupt received → stopping all")
            self.stop_all()
            
    # --------------------------
    # Reset 
    # --------------------------
    def reset_for_new_scan(self):
        self._manager_stop_event.clear()
        self._threads.clear()
        self._watch_dir = None
        self._file_timestamps.clear()
        self._watcher_thread = None
        self._reload_queue.clear()
        self._initial_scan_done = False
        self.logger.logMessage("[ThreadManager] Reset Complete")
