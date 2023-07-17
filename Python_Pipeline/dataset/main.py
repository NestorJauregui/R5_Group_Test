from dataset.clients import Clients
from dataset.stores import Stores
from dataset.neighborhoods import Neighborhoods
from dataset.purchases import Purchases


class DataSet:
    def __init__(self, raw_data, db_client):
        self.raw_data = raw_data
        self.db_client = db_client

        self.clients_client = Clients(self.raw_data, self.db_client)
        self.stores_client = Stores(self.raw_data, self.db_client)
        self.neighborhoods_client = Neighborhoods(self.raw_data, self.db_client)
        self.purchases_client = Purchases(self.raw_data, self.db_client)

    def insert_clients(self):
        self.clients_client.insert_clients()

    def insert_stores(self):
        self.stores_client.insert_stores()

    def insert_neighborhoods(self):
        self.neighborhoods_client.insert_neighborhoods()

    def insert_purchases(self):
        self.purchases_client.insert_purchases()

