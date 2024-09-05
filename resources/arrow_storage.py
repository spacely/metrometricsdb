"""
Module for processing GTFS data files and converting them to Apache Arrow format.
"""
from typing import Dict
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

class ArrowProcessor:
    def __init__(self, data: Dict[str, pd.DataFrame]):
        """
        Initialize the class with the data to be converted.

        Args:
            data: A dictionary where keys are strings representing the data type
                  and values are pandas DataFrames to be converted.
        """
        self.data = data
        self.arrow_tables = {}

    def convert_to_arrow(self) -> None:
        """Convert data from pandas DataFrame format to Apache Arrow Table format."""
        self.arrow_tables = {key: pa.Table.from_pandas(df) for key, df in self.data.items()}

    def join_trips_and_routes(self) -> None:
        """Join Trips and Routes tables if they exist."""
        if "trips" in self.arrow_tables and "routes" in self.arrow_tables:
            trips_table = self.arrow_tables["trips"].to_pandas()
            routes_table = self.arrow_tables["routes"].to_pandas()
            trips_routes = trips_table.merge(routes_table, on="route_id", how="left")
            self.arrow_tables["trips_routes"] = pa.Table.from_pandas(trips_routes)

    def join_trips_and_stop_times(self) -> None:
        """Join Trips and Stop Times tables if they exist."""
        if "trips" in self.arrow_tables and "stop_times" in self.arrow_tables:
            trips_table = self.arrow_tables["trips"].to_pandas()
            stop_times_table = self.arrow_tables["stop_times"].to_pandas()
            trips_stop_times = trips_table.merge(stop_times_table, on="trip_id", how="left")
            self.arrow_tables["trips_stop_times"] = pa.Table.from_pandas(trips_stop_times)

    def process_data(self) -> Dict[str, pa.Table]:
        """
        Process the data by converting to Arrow and performing necessary joins.

        Returns:
            A dictionary where keys are strings representing the data type
            and values are Apache Arrow Tables.
        """
        print("Processing data with Apache Arrow...")
        self.convert_to_arrow()
        self.join_trips_and_routes()
        self.join_trips_and_stop_times()
        return self.arrow_tables

    def save_arrow_data(self, output_dir: str) -> None:
        """
        Save Arrow tables to Parquet files.

        Args:
            output_dir: Directory to save the Parquet files.
        """
        for key, table in self.arrow_tables.items():
            pq.write_table(table, f"{output_dir}/{key}.parquet")

    def retrieve_arrow_data(self, input_dir: str) -> Dict[str, pa.Table]:
        """
        Retrieve Parquet files and convert to Arrow tables.

        Args:
            input_dir: Directory containing the Parquet files.

        Returns:
            A dictionary of Arrow tables.
        """
        retrieved_tables = {}
        for key in self.arrow_tables.keys():
            retrieved_tables[key] = pq.read_table(f"{input_dir}/{key}.parquet")
        return retrieved_tables
