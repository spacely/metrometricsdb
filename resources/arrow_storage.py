"""
Module for processing GTFS data files and converting them to Apache Arrow format.
"""
import pyarrow as pa


class ArrowProcessor:  # pylint: disable=R0903
    """
    A class that manages the logic of converting GTFS files in CSV format to Apache Arrow.
    """

    def __init__(self, data):
        """
        Initialize the class with the data to be converted.

        Args:
            data (dict): A dictionary where keys are strings representing the data type
                         and values are pandas DataFrames to be converted.
        """
        self.data = data
        # Subscribe to a specific message type that IOManager publishes
        # pub.subscribe(self.process_data, "data_ready")

    def process_data(self):
        """
        Convert data from pandas DataFrame format to Apache Arrow Table format.

        Returns:
            dict: A dictionary where keys are strings representing the data type
                  and values are Apache Arrow Tables.
        """
        print("Processing data with Apache Arrow...")
        arrow_tables = {key: pa.Table.from_pandas(df) for key, df in self.data.items()}

        # Join Trips and Routes
        if "trips" in arrow_tables and "routes" in arrow_tables:
            trips_table = arrow_tables["trips"].to_pandas()
            routes_table = arrow_tables["routes"].to_pandas()
            trips_routes = trips_table.merge(routes_table, on="route_id", how="left")
            arrow_tables["trips_routes"] = pa.Table.from_pandas(trips_routes)

        # Join Trips and Stop Times
        if "trips" in arrow_tables and "stop_times" in arrow_tables:
            trips_table = arrow_tables["trips"].to_pandas()
            stop_times_table = arrow_tables["stop_times"].to_pandas()
            trips_stop_times = trips_table.merge(
                stop_times_table, on="trip_id", how="left"
            )
            arrow_tables["trips_stop_times"] = pa.Table.from_pandas(trips_stop_times)

        print("Data processed and combined into Arrow format.")
        print(arrow_tables)
        return arrow_tables
        # Publish processed data for further use or storage
        # pub.sendMessage("arrow_data_ready", data=arrow_tables)

    def save_arrow_data(self):
        """
        Saves arrow to parquuet
        """
        return None

    def retrieve_arrow_data(self):
        """
        Retrieves parquet and convert to arrow
        """
        return None
