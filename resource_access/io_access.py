from resources import filessystem_storage
class IOManager:
    def __init__(self, message_bus):
        self.message_bus = message_bus
        self.message_bus.subscribe('load_gtfs', self.handle_load_gtfs)

    def handle_load_gtfs(self, folder_path):
        print(f"Received request to load GTFS data from: {folder_path}")
        # Assuming FileSystemHandler has a method `read_folder` that handles the reading of files.
        fs_storage = filessystem_storage.FileSystemStorage(folder_path)
        gtfs_data = fs_storage.load_gtfs_files()
        print("Loaded GTFS data:", gtfs_data)
        # Now send this data over the message bus
        self.message_bus.publish('data_ready', data=gtfs_data)
        # Process the data or pass it to another component
        print("GTFS data loaded and processed")