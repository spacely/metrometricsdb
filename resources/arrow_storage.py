import pyarrow as pa
from pubsub import pub


class ArrowProcessor:
    def __init__(self, data):
        self.data = data
        # Subscribe to a specific message type that IOManager publishes
        #pub.subscribe(self.process_data, "data_ready")

    def process_data(self):
        # Assuming 'data' is a dictionary of pandas DataFrames
        print("Processing data with Apache Arrow...")
        arrow_tables = {key: pa.Table.from_pandas(df) for key, df in self.data.items()}
        # Here you could add further processing, saving to Parquet, etc.
        print("Data processed and converted to Arrow format.")

        # Publish processed data for further use or storage
        #pub.sendMessage("arrow_data_ready", data=arrow_tables)
