from dataset.main import DataSet
from database.google_sheets_data_downloader import GoogleSheetsDataDownloader
from database.db import DataBase


def get_and_load_data_etl():

    google_sheets_data_downloader = GoogleSheetsDataDownloader()
    raw_data_df = google_sheets_data_downloader.get_complete_data()

    eng = DataBase()
    eng.connect()

    dataset = DataSet(raw_data_df, eng)
    dataset.insert_clients()
    dataset.insert_stores()
    dataset.insert_neighborhoods()
    dataset.insert_purchases()

    eng.disconnect()

    print("Data loaded successfully")


if __name__ == "__main__":
    get_and_load_data_etl()


