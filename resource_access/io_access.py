# io_access.py
"""
This module provides access to IO functionalities necessary for loading and processing GTFS data.
It includes functionality to load data from files and convert it into Apache Arrow format.
"""
from resources import filessystem_storage, arrow_storage


class IOAccess:
    """
    A class to handle data access operations including loading data
    from the filesystem and converting data to Arrow format.
    """

    def __init__(self):
        """
        Initialize the IOAccess class.
        """
        return

    def load_data(self, folder_path):
        """
        Load data from a specified folder using the FileSystemStorage.

        Args:
            folder_path (str): The path to the folder containing GTFS files.

        Returns:
            dict: Loaded GTFS data.
        """
        storage_load = filessystem_storage.FileSystemStorage(folder_path)
        print(f"Accessing GTFS data from the folder: {folder_path}")
        try:
            gtfs_data = storage_load.load_gtfs_files()
            print("The GTFS files", type(gtfs_data))
            print("Data accessed and ready for processing.")
            return gtfs_data
        except Exception as io_error:
            print(f"Failed to access GTFS data: {io_error}")
            raise

    def convert_to_arrow(self, data):
        """
        Convert data to Arrow format.

        Args:
            data (dict): The data to be converted.

        Returns:
            pyarrow.Table: The data converted to Arrow format.
        """
        arrow_format = arrow_storage.ArrowProcessor(data)
        arrow_data = arrow_format.process_data()
        print("Data converted to arrow format")
        return arrow_data
