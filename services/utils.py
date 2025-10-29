# utils.py
import json
import os
import time
from dataclasses import is_dataclass, fields, is_dataclass
from typing import get_type_hints, List, Union, TypeVar, Dict, Any, Type, Union
import tempfile
from pathlib import Path
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from services.logging.logger_singleton import getLogger




def load_json_cache(file_path, max_age_seconds=86400):
    if not os.path.exists(file_path):
        return None
    stat = os.stat(file_path)
    if time.time() - stat.st_mtime > max_age_seconds:
        return None
    with open(file_path, "r") as f:
        return json.load(f)

def save_json_cache(file_path, data):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(data, f)

def get_boolean_input(prompt_message: str,defaultValue: bool = False, defaultOnEnter:bool = True):
    while True:
        user_input = input(prompt_message + "\n").lower()
        if user_input == "true":
            return True
        elif user_input == "false":
            return False
        elif user_input in ("", None) and defaultOnEnter:
            validate = yes_no(f"Take the default value ({defaultValue})?")
            if validate:
                return defaultValue
        else:
            print("Invalid input. Please enter 'True' or 'False'.")
            
            
def yes_no(prompt: str, defaultResponse: bool = True, defaultOnEnter:bool = True) -> bool:
    defaultYn = "Yes" if defaultResponse else "No"
    while True:
        response = input(f"{prompt} (y/n): ").strip().lower()
        if response in ("y", "yes"):
            return True
        elif response in ("n", "no"):
            return False
        elif response in ("",None):
            return defaultResponse
        else:
            print("Please enter 'y' or 'n'.")
            

T = TypeVar("T")

def from_dict(cls: Type[T], data: Union[Dict[str, Any], List[Any]]) -> T:
    """
    Recursively converts a dict (or list of dicts) into dataclass instances.
    Handles nested dataclasses, lists, and Optional fields.
    """
    # If data is a list, convert each element
    if isinstance(data, list):
        # Attempt to get the inner type if cls is typing.List
        if hasattr(cls, "__origin__") and cls.__origin__ == list and hasattr(cls, "__args__"):
            inner_type = cls.__args__[0]
            return [from_dict(inner_type, item) for item in data]
        else:
            # Fallback: just return the list as-is
            return data

    # If cls is not a dataclass, return data directly
    if not is_dataclass(cls):
        return data

    # cls is a dataclass, get type hints
    type_hints = get_type_hints(cls)

    # Build a dict of field values
    init_values = {}
    for f in fields(cls):
        field_name = f.name
        field_type = type_hints.get(field_name, f.type)

        if field_name not in data or data[field_name] is None:
            init_values[field_name] = None
            continue

        value = data[field_name]

        # Handle Optional[T]
        origin = getattr(field_type, "__origin__", None)
        args = getattr(field_type, "__args__", ())

        if origin is Union and type(None) in args:
            # Optional[T] -> unwrap the inner type
            inner_type = args[0] if args[0] != type(None) else args[1]
            init_values[field_name] = from_dict(inner_type, value)
        # Handle List[T]
        elif origin is list and args:
            inner_type = args[0]
            init_values[field_name] = [from_dict(inner_type, v) for v in value]
        # Handle nested dataclass
        elif is_dataclass(field_type):
            init_values[field_name] = from_dict(field_type, value)
        else:
            # Primitive type, assign directly
            init_values[field_name] = value

    return cls(**init_values)



DEFAULT_FLAG = Path.cwd() / ".scanner_reload"

def _resolve_path(path: Union[str, Path, None]) -> Path:
    return Path(path).expanduser() if path else DEFAULT_FLAG

def set_reload_flag(path: Union[str, Path, None] = None, content: str = "1") -> bool:
    """
    Create or overwrite the flag file atomically.
    Returns True on success, False on error.
    """
    flag_path = _resolve_path(path)
    flag_dir = flag_path.parent
    try:
        flag_dir.mkdir(parents=True, exist_ok=True)
        # Create a temp file in the same directory to ensure atomic move/replace works across filesystems.
        with tempfile.NamedTemporaryFile("w", delete=False, dir=str(flag_dir), prefix=".tmp_flag_") as tf:
            tf.write(content)
            tf.flush()
            os.fsync(tf.fileno())
            tmpname = tf.name
        # Atomic replace (works on Windows and Unix)
        os.replace(tmpname, str(flag_path))
        return True
    except Exception as e:
        # Log or handle as you prefer; return False for caller to react
        # print("Failed to set reload flag:", e)
        try:
            # best-effort cleanup of temp file
            if 'tmpname' in locals() and os.path.exists(tmpname):
                os.remove(tmpname)
        except Exception:
            pass
        return False

def clear_reload_flag(path: Union[str, Path, None] = None) -> bool:
    """
    Remove the flag file if present.
    Returns True if removed or didn't exist, False on error.
    """
    flag_path = _resolve_path(path)
    try:
        if flag_path.exists():
            flag_path.unlink()
        return True
    except Exception:
        return False

def is_reload_flag_set(path: Union[str, Path, None] = None) -> bool:
    """
    Check whether the flag file exists and (optionally) has non-empty content.
    """
    flag_path = _resolve_path(path)
    try:
        if not flag_path.exists():
            return False
        # Optional: check content instead of mere existence
        content = flag_path.read_text().strip()
        return bool(content)
    except Exception:
        return False

def get_project_root_os():
    current_file_path = os.path.abspath(__file__)
    # Traverse up until a recognizable project root indicator is found
    # This example looks for a .git directory or a specific project file
    while True:
        parent_dir = os.path.dirname(current_file_path)
        if not parent_dir or parent_dir == current_file_path:
            # Reached the filesystem root or a loop
            return None
        if os.path.exists(os.path.join(parent_dir, '.git')) or \
           os.path.exists(os.path.join(parent_dir, 'pyproject.toml')) or \
           os.path.exists(os.path.join(parent_dir, 'setup.py')):
            return parent_dir
        current_file_path = parent_dir



def is_json(value):
    """
    Returns True if `value` is a JSON string (object or array), False otherwise.
    """
    if not isinstance(value, str):
        return False
    try:
        json.loads(value)
        return True
    except json.JSONDecodeError:
        return False


# Lock to ensure thread safety
_scratch_lock = threading.Lock()

# Directory to store scratch logs
SCRATCH_DIR = Path("scratch_logs")
SCRATCH_DIR.mkdir(exist_ok=True)

def write_scratch(message: str, filename: str = None):
    """
    Append a message to the daily scratch log in a thread-safe manner.
    
    :param message: Message to write.
    :param filename: Optional custom filename (defaults to date-based).
    """
    now = datetime.now()
    # Default filename: scratch_YYYY-MM-DD.log
    file_path = SCRATCH_DIR / (filename or f"scratch_{now.date()}.log")
    
    # Format the message with timestamp
    line = f"[{now.isoformat()}] {message}\n"
    
    # Thread-safe write
    with _scratch_lock:
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(line)



def get_job_count():
    cores = os.cpu_count() or 1
    # You can tune the scaling here:
    if cores <= 4:
        return cores  # Pi or small system → use all cores
    else:
        return min(8, cores)  # Mac or bigger system → cap at 8



# ------------------------- Generic parallel runner -------------------------
def run_parallel(fn, items, stop_event=None, collect_errors=True):
    results, errors = [], []
    lock = threading.Lock()
    logger = getLogger()
    
    max_workers = int(max(1,get_job_count()))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fn, item): item for item in items}
        for fut in as_completed(futures):
            if stop_event and stop_event.is_set():
                break
            try:
                res = fut.result()
                if res is not None:
                    with lock:
                        results.append(res)
            except Exception as e:
                logger.logMessage(f"[run_parallel] {e}")
                if collect_errors:
                    errors.append((futures[fut], e))
                else:
                    raise
    return results, errors