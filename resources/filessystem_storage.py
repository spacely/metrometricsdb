"""
A module for retrieving GTFS files from a specified directory.
"""

import os
import logging
import pandas as pd


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class FileSystemStorage:  # pylint: disable=R0903
    """
    Class for handling the retrieval of GTFS files from a filesystem.
    """

    def __init__(self, base_folder):
        """
        Initialize the FileSystemStorage class with a directory path.

        Args:
            base_folder (str): The base directory from which GTFS files are loaded.
        """
        self.base_folder = base_folder

    def load_file(self, filename):
        """
        Load a single file from the filesystem.

        Args:
            filename (str): The name of the file to load.

        Returns:
            pd.DataFrame or None: The loaded data as a pandas DataFrame,
            or None if the file cannot be loaded.
        """
        logging.info("Loading data from %s", filename)

        file_path = os.path.join(self.base_folder, filename)
        logging.info("Attempting to load data from %s", file_path)

        try:
            return pd.read_csv(file_path)
        except FileNotFoundError as file_not_found:
            logging.error("Failed to load data from %s: %s", file_path, file_not_found)
            print(f"File not found: {file_path}")

            return None

    def load_gtfs_files(self):
        """
        Load all necessary GTFS files specified in a dictionary.

        Returns:
            dict: A dictionary containing data from various GTFS files.
        """
        gtfs_files = {
            "agency": "agency.txt",
            "stops": "stops.txt",
            "routes": "routes.txt",
            "trips": "trips.txt",
            "stop_times": "stop_times.txt",
            "calendar": "calendar.txt",
            "calendar_dates": "calendar_dates.txt",
            "fare_attributes": "fare_attributes.txt",
            "fare_rules": "fare_rules.txt",
            "shapes": "shapes.txt",
        }
        data = {}
        for key, filename in gtfs_files.items():
            loaded_data = self.load_file(filename)
            if loaded_data is not None:
                data[key] = loaded_data

        return data


def store_arrow_data():  # pylint: disable=R0903
    """
    Store data in Apache Arrow format at a specified base path.
    """
    return None
    # Implementation here


def retrieve_arrow_data():  # pylint: disable=R0903
    """
    Retrieve data in Apache Arrow format from a specified base path.
    """

    return None
    # Implementation here
