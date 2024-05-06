import pandas as pd
import os
import logging


class FileSystemStorage:
    def __init__(self, base_folder):
        self.base_folder = base_folder

    def load_file(self, filename):
        """ Utility function to load a single file """
        logging.info(f"Loading data from {file_path}")
        file_path = os.path.join(self.base_folder, filename)
        try:
            return pd.read_csv(file_path)
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            logging.error(f"Failed to load data from {file_path}: {str(e)}")
            return None
        except Exception as e:
            print(f"Error reading {file_path}: {str(e)}")
            logging.error(f"Failed to load data from {file_path}: {str(e)}")
            return None

    def load_gtfs_files(self):
        """ Load all necessary GTFS files """
        gtfs_files = {
            'agency': 'agency.txt',
            'stops': 'stops.txt',
            'routes': 'routes.txt',
            'trips': 'trips.txt',
            'stop_times': 'stop_times.txt',
            'calendar': 'calendar.txt',
            'calendar_dates': 'calendar_dates.txt',
            'fare_attributes': 'fare_attributes.txt',
            'fare_rules': 'fare_rules.txt',
            'shapes': 'shapes.txt',
            'frequencies': 'frequencies.txt',
            'transfers': 'transfers.txt',
            'feed_info': 'feed_info.txt'
        }
        data = {}
        for key, filename in gtfs_files.items():
            data[key] = self.load_file(filename)
        return data