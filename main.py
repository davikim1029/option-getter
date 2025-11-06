# main.py
import os
import sys
import time as pyTime
import argparse
from dotenv import load_dotenv
from services.utils import yes_no
from services.etrade_consumer import force_generate_new_token
from services.logging.logger_singleton import getLogger
from services.core.shutdown_handler import ShutdownManager
from services.scanner.scanner_entry import start_scanner
from services.threading.thread_manager import ThreadManager
from services.utils import is_reload_flag_set,clear_reload_flag


import sys
import os
import subprocess
import signal
from pathlib import Path
from collections import deque
from logging import FileHandler
from pathlib import Path
from collections import deque
from logging import FileHandler
# -----------------------------
# Paths & Constants
# -----------------------------
PID_FILE = Path("scanner_server.pid")
MAIN_FILE = Path("main.py")  # entrypoint of your scanner
SCANNER_CMD = [sys.executable, str(MAIN_FILE), "--mode", "scan"]

logger = getLogger()
LOG_FILE = Path("option_getter_server.log")
for handler in logger.logger.handlers:
    if isinstance(handler, FileHandler):
        LOG_FILE = Path(handler.baseFilename)
        break # Assuming you only care about the first FileHandler found



# -----------------------------
# Helper Functions
# -----------------------------
def is_scanner_running():
    if not PID_FILE.exists():
        return False
    try:
        pid = int(PID_FILE.read_text())
        os.kill(pid, 0)
        return True
    except (ValueError, ProcessLookupError):
        PID_FILE.unlink(missing_ok=True)
        return False


def start_scanner_server():
    if is_scanner_running():
        print("Scanner is already running.")
        return

    if PID_FILE.exists():
        PID_FILE.unlink(missing_ok=True)

    with LOG_FILE.open("a") as log_file:
        process = subprocess.Popen(
            SCANNER_CMD,
            stdout=log_file,
            stderr=log_file,
        )

    PID_FILE.write_text(str(process.pid))
    print(f"Scanner started with PID {process.pid}, logging to {LOG_FILE}")


def stop_scanner_server():
    if not PID_FILE.exists():
        print("No PID file found. Scanner may not be running.")
        return

    pid = int(PID_FILE.read_text())
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to PID {pid}")
        for _ in range(10):
            pyTime.sleep(0.5)
            os.kill(pid, 0)
    except ProcessLookupError:
        print("Process not found (already stopped).")
    PID_FILE.unlink(missing_ok=True)
    print("Scanner stopped.")


def check_scanner_server():
    if is_scanner_running():
        pid = int(PID_FILE.read_text())
        print(f"Scanner is running with PID {pid}")
    else:
        print("Scanner is not running.")


# Disable GPU / MPS fallback
os.environ["CUDA_VISIBLE_DEVICES"] = ""
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"



def get_mode_from_prompt():
    """
    Interactive mode selection for CLI.
    """
    modes = [
        ("scan", "Run scanner (alerts only)"),
        ("refresh-token", "Refresh the Etrade token"),
        ("start-server", "Start the alerts server"),
        ("stop-server","Stop the alerts server"),
        ("check-server","Check the alerts server status"),
        ("quit", "Exit program")
    ]

    while True:
        print("Available modes:")
        for i, (key, desc) in enumerate(modes, start=1):
            print(f"  {i}. {desc} [{key}]")
        
        choice = input("\nEnter mode number (default 1): ").strip()
        
        if choice in ("q", "quit"):
            print("Exiting program.")
            return "quit"
        
        if not choice:
            run_scan = yes_no("Run Scan? (default is yes)")
            if run_scan:
                return "scan"
            else:
                continue
        try:
            index = int(choice) - 1
            if 0 <= index < len(modes):
                return modes[index][0]
        except ValueError:
            pass
        print("Invalid choice, try again.")
          
    #If we somehow are here, exit
    return "quit"



def main():
    # Ensure directories exist
    os.makedirs("cache", exist_ok=True)
    logger = getLogger()
    logger.logMessage("Script started.")
    
    load_dotenv()

    parser = argparse.ArgumentParser(description="OptionsAlerts CLI")
    parser.add_argument("--mode", help="Mode to run")
    args = parser.parse_args()
    
    while True:
        
        if is_reload_flag_set():
            clear_reload_flag()
            
            #Wait for threading to reset
            manager = ThreadManager.instance()
            logger.logMessage("Resetting thread manager")
            manager.reset_for_new_scan()                      
            logger.logMessage("Scanner restarting")
            start_scanner()
            
        else:              
            mode = args.mode.lower() if args.mode else get_mode_from_prompt()
            args.mode = None #After get it mode the first time, reset for additional iterations
            
            if mode == "quit":
                ThreadManager.instance().stop_all()
                sys.exit(0)
                break
            
            # --- Mode Handling ---
            if mode == "scan":
                # Initialize shutdown manager
                ShutdownManager.init(error_logger=logger.logMessage)
                tm = ThreadManager.instance()
                tm.reset_for_new_scan()
                start_scanner()
                tm.wait_for_shutdown()
                
            elif mode == "refresh-token":
                force_generate_new_token()
                
            elif mode == "start-server":
                start_scanner_server()                
                
            elif mode == "stop-server":
                stop_scanner_server()
             
            elif mode == "check-server":
                check_scanner_server()
                       
                        
            else:
                print("Invalid mode selected.")
          
        #force shutdown of threads  
        ThreadManager.instance().stop_all()



if __name__ == "__main__":
    main()
