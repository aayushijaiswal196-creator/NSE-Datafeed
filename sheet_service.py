import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# Define the scope
scope = ['https://www.googleapis.com/auth/spreadsheets']

base_dir = os.path.dirname(os.path.abspath(__file__))
SERVICE_ACCOUNT_FILE = os.path.join(base_dir, "service_account.json")
SPREADSHEET_ID = "1ZbIM2t5x-g7KyaZFxw635-foX0AkK0ET2R2quUk6G8Y"

def process_and_upload_data(file_path):
    print(f"Processing file for upload: {file_path}")
    
    # Extract filename without extension for worksheet name
    filename = os.path.basename(file_path)
    #worksheet_name = os.path.splitext(filename)[0]
    worksheet_name = "Sheet1"
    print(f"Using worksheet name: {worksheet_name}")
    
    # Use the credentials file you created
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
    client = gspread.authorize(creds)
    
    try:
        # Read the CSV with specific columns (indices 1, 2, 5, 6, 7, 8) corresponding to B, C, F, G, H, I
        # Note: usecols argument in read_csv expects 0-based indices or column names.
        # B=1, C=2, F=5, G=6, H=7, I=8
        df = pd.read_csv(file_path)
        
        # Remove the first two rows (which might be metadata or unwanted headers)
        df = df.iloc[1:]
        
        # Select columns by integer location
        # Since we removed rows, indices might be reset or not. iloc works on position.
        # We need to ensure we are selecting columns by position.
        df = df.iloc[:, [1, 2, 5, 6, 7, 8]]
        
        df = df.fillna("")
        
        sheet = client.open_by_key(SPREADSHEET_ID)
        try:
            worksheet = sheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            print(f"Worksheet '{worksheet_name}' not found. Creating new one.")
            worksheet = sheet.add_worksheet(worksheet_name, rows=str(len(df)), cols=str(len(df.columns)))

        worksheet.clear()
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"Data updated successfully to worksheet: {worksheet_name}")
        return True
    except Exception as e:
        print(f"Error uploading to Google Sheet: {e}")
        return False

#custom function to upload to cell sheet

def write_to_cell_sheet(spreadsheet_id, sheet_id, cell, value):
    """
    Updates a single cell in a Google Sheet with the provided value.
    """
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
    try:
        service = build("sheets", "v4", credentials=creds)
        range_name = f"{sheet_id}!{cell}"
        body = {"values": [[value]]}
        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="USER_ENTERED",
                body=body,
            )
            .execute()
        )
        print(f"{result.get('updatedCells')} cells updated.")
        return result
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error
    
def read_from_cell_sheet(spreadsheet_id, sheet_id, cell):
    """
    Reads a single cell from a Google Sheet.
    """
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
    try:
        service = build("sheets", "v4", credentials=creds)
        range_name = f"{sheet_id}!{cell}"
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_name)
            .execute()
        )
        rows = result.get("values", [])
        print(f"{len(rows)} rows retrieved")
        if not rows or not rows[0]:
            return None
        return rows[0][0]
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error

def shift_column_range(spreadsheet_id, sheet_id, source_range, destination_range):
    """
    Moves values from a source range to a destination range by reading, writing, and then clearing the source.
    """
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
    try:
        service = build("sheets", "v4", credentials=creds)
        
        # 1. Get values from source
        source_full_range = f"{sheet_id}!{source_range}"
        print(f"Reading from {source_full_range}")
        get_result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=source_full_range
        ).execute()
        values = get_result.get('values', [])
        
        if not values:
            print(f"No data found in {source_full_range}")
            # Even if source is empty, we should clear destination to reflect the 'move' of empty cells
            destination_full_range = f"{sheet_id}!{destination_range}"
            service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id, range=destination_full_range
            ).execute()
            return get_result
        
        # 2. Update destination
        destination_full_range = f"{sheet_id}!{destination_range}"
        print(f"Writing to {destination_full_range}")
        body = {"values": values}
        update_result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=destination_full_range,
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        print(f"{update_result.get('updatedCells')} cells updated in destination.")

        # 3. Clear source
        print(f"Clearing source {source_full_range}")
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=source_full_range
        ).execute()
        
        return update_result

    except HttpError as error:
        print(f"An error occurred: {error}")
        return error


if __name__ == "__main__":
    # For testing purposes, you can uncomment and provide a path
    # from getdate import get_current_date
    # download_dir = os.path.join(base_dir, "Download")
    # filename= "fao_participant_oi_"+get_current_date()+".csv"
    # process_and_upload_data(os.path.join(download_dir, filename))
    pass
