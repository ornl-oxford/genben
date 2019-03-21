from dask.distributed import Client


class DaskUtils:
    def __init__(self):
        self.client = None

    def connect_to_scheduler(self, address='127.0.0.1', port=8786):
        # Connect to Dask scheduler
        print('[Dask Utils] Connecting to Dask scheduler at {address}:{port}'.format(address=address, port=port))
        self.client = Client('{}:{}'.format(address, port))
