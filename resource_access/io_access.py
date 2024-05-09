# io_access.py
from resources import filessystem_storage, arrow_storage


class IOAccess:
    def __init__(self):
        return

    def load_data(self, folder_path):
        """
        Load data from a specified folder using the FileSystemStorage.
        """
        storage_load = filessystem_storage.FileSystemStorage(folder_path)
        print(f"Accessing GTFS data from the folder: {folder_path}")
        try:
            gtfs_data = storage_load.load_gtfs_files()
            print("Data accessed and ready for processing.")
            return gtfs_data
        except Exception as e:
            print(f"Failed to access GTFS data: {e}")
            raise

    def convert_to_arrow(self, data):
        """
        Convert data to Arrow format

        """
        arrow_format = arrow_storage.ArrowProcessor(data)
        arrow_data = arrow_format.process_data()
        print("Data converted to arrow format")
        return arrow_data
