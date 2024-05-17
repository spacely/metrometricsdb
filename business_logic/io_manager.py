"""
A class that manages all IO related activities within the engine.
"""
from pubsub import pub
from resource_access import io_access


class IOManager:
    """
    IOManager that coordinates access to IO and sends results
    to the QueryManager when Apache Arrow data is loaded.
    """

    def __init__(self):
        """
        Initialize the IO manager
        """
        # Subscribe to 'load_gtfs' topic to handle loading GTFS data requests
        self.resource_access = io_access.IOAccess()
        pub.subscribe(self.handle_load_gtfs, "load_gtfs")
        pub.subscribe(self.convert_to_arrow_format, "data_ready")

    def handle_load_gtfs(self, folder_path):
        """
        Loads the GTFS files from the folder location provided
        """
        print(f"Received request to load GTFS data from: {folder_path}")

        gtfs_data = self.resource_access.load_data(folder_path)

        # print("Loaded GTFS data:", gtfs_data)

        # Now send this data over the message bus
        pub.sendMessage("data_ready", loaded_data=gtfs_data)

        # Process the data or pass it to another component
        print("GTFS data loaded and processed")

    def convert_to_arrow_format(self, loaded_data):
        """
        Convert the loaded GTFS files into Apache Arrow
        """
        arrow_data = self.resource_access.convert_to_arrow(loaded_data)
        print("Data converted to Arrow format, read for query")
        pub.sendMessage("query_ready", data=arrow_data)
