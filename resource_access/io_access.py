# io_access.py
from resources import filessystem_storage,arrow_storage

class IOAccess:
    def __init__(self):
        self.storage = filessystem_storage.FileSystemStorage()
        self.arrow_format = arrow_storage.ArrowProcessor()

    def load_data(self, folder_path):
        """
        Load data from a specified folder using the FileSystemStorage.
        """
        print(f"Accessing GTFS data from the folder: {folder_path}")
        try:
            gtfs_data = self.storage.load_gtfs_files(folder_path)  # Assuming this function exists
            print("Data accessed and ready for processing.")
            return gtfs_data
        except Exception as e:
            print(f"Failed to access GTFS data: {e}")
            raise
    
    def convert_to_arrow(self,data):
        """
        Convert data to Arrow format
    
        """
        arrow_data = self.arrow_format.process_data(data)
        print("Data converted to arrow format")
        return arrow_data