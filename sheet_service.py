import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

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

if __name__ == "__main__":
    # For testing purposes, you can uncomment and provide a path
    # from getdate import get_current_date
    # download_dir = os.path.join(base_dir, "Download")
    # filename= "fao_participant_oi_"+get_current_date()+".csv"
    # process_and_upload_data(os.path.join(download_dir, filename))
    pass
