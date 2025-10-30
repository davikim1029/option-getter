# services/core/option_data_manager.py
from services.core.file_manager import FileManager
from pathlib import Path

class OptionDataManager:
    """
    Manages option data records using a buffered FileManager.
    """

    def __init__(self, filepath: str = "data/option_data/option_data.json"):
        self.filepath = Path(filepath)
        self.file_manager = FileManager(
            filepath=self.filepath,
            flush_interval=30,
            max_buffer_size=500
        )

    def add_option_record(self, record: dict):
        """Queue an option record for writing."""
        self.file_manager.add_entry(record)

    def close(self, combine_temp: bool = True):
        """
        Flush remaining data and stop FileManager.
        If combine_temp=True, merge all temp files into the final JSON array.
        """
        self.file_manager.close()
        if combine_temp:
            self.file_manager.combine_temp_files()
