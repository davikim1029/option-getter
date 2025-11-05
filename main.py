# main.py
import os
import sys
from dotenv import load_dotenv
from services.utils import yes_no
from services.etrade_consumer import force_generate_new_token
from services.logging.logger_singleton import getLogger
from services.core.shutdown_handler import ShutdownManager
from services.scanner.scanner_entry import start_scanner
from services.threading.thread_manager import ThreadManager
from services.utils import is_reload_flag_set,clear_reload_flag


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
            mode = get_mode_from_prompt()
            
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
                        
            else:
                print("Invalid mode selected.")
          
        #force shutdown of threads  
        ThreadManager.instance().stop_all()



if __name__ == "__main__":
    main()
