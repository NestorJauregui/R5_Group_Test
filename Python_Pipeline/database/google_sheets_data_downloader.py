import os
import pickle

import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build


class GoogleSheetsDataDownloader:
    def __init__(self):
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self.SPREADSHEET_ID = "16ZsVERVrw1aGiksEL-GlinuRSFpDt3Z2ExM22jFB0jo"
        self.SHEET_TO_POOL = "Hoja 1"
        self.creds = None
        self.raw_data = None

    def gsheet_api_check(self):
        # Function that makes the credentials checks for the Google Sheets API
        creds = None
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('client.json', self.SCOPES)
                creds = flow.run_local_server(port=0)
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        self.creds = creds

    def pull_sheet_data(self):
        self.gsheet_api_check()
        service = build('sheets', 'v4', credentials=self.creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=self.SPREADSHEET_ID, range=self.SHEET_TO_POOL
        ).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
        else:
            rows = sheet.values().get(
                spreadsheetId=self.SPREADSHEET_ID, range=self.SHEET_TO_POOL
            ).execute()
            data = rows.get('values')
            print("COMPLETE: Data copied")
            self.raw_data = data

    def get_complete_data(self):
        self.pull_sheet_data()
        df = pd.DataFrame(self.raw_data[1:], columns=self.raw_data[0])
        return df
