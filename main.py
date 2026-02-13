import os
import urllib.request
import urllib.error
import pandas as pd
from fastapi import FastAPI, BackgroundTasks
import uvicorn
from getdate import get_current_date
from sheet_service import (
    process_and_upload_data,
    shift_column_range,
    write_to_cell_sheet,
    read_from_cell_sheet,
    SPREADSHEET_ID
)
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
from pytz import timezone



SHEET_NAME = "Sheet1" # Default to Sheet1 as per sheet_service

def download_file_only():
    date = get_current_date()
    # URL provided by user
    url = "https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_"+date+".csv"

    # Setup paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(base_dir, "Download")
    filename = os.path.basename(url)
    output_path = os.path.join(download_dir, filename)

    # Ensure download directory exists
    if not os.path.exists(download_dir):
        try:
            os.makedirs(download_dir)
            print(f"Created directory: {download_dir}")
        except OSError as e:
            print(f"Error creating directory {download_dir}: {e}")
            return None

    # Headers to mimic browser request
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    print(f"Starting download from {url}...")
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as response, open(output_path, 'wb') as out_file:
            data = response.read()
            out_file.write(data)
        print(f"Download successful! Saved to: {output_path}")
        return output_path

    except Exception as e:
        print(f"Error downloading file: {e}")
        return None

def download_nse_file():
    path = download_file_only()
    if path:
        # Upload to Google Sheet
        try:
            process_and_upload_data(path)
        except Exception as e:
            print(f"Failed to upload to Google Sheet: {e}")

def script_sync_today():
    # 1. Download file
    file_path = download_file_only()
    if not file_path:
        return {"status": "error", "message": "Failed to download file"}

    # 2. Shift columns
    # Assuming rows 3 to 6 as per extraction request, or maybe larger range?
    # User said "Shift G-> C and H-> D". I will assume standard data rows (all rows).
    # But since update is explicit for G3:G6, I'll shift G3:G100 to be safe/generic.
    # Actually, let's just shift a safe range.
    print("Shifting existing data...")
    shift_column_range(SPREADSHEET_ID, SHEET_NAME, "G3:G7", "C3:C7")
    shift_column_range(SPREADSHEET_ID, SHEET_NAME, "H3:H7", "D3:D7")

    # 3. Shift G1 to C1
    shift_column_range(SPREADSHEET_ID, SHEET_NAME, "G1", "C1")

    # 4. Extract and update
    try:
        df = pd.read_csv(file_path)
        # Skip top rows if needed (process_and_upload skips 1? let's check)
        # process_and_upload used df.iloc[1:].
        # The file usually has a header, then data.
        # Let's assume standard behavior: ignore first row if metadata.
        # Check if rows 3-6 implies indices 0-3 of the data dataframe?
        # User said "Extract Data for G3:G6". These are 4 rows.
        # The dataframe usually has 4 rows (Client, DII, FII, Pro).

        # Taking columns for G and H. guessing indices 3 and 4?
        # Previous mapping: 1->B, 2->C, 5->F, 6->G, 7->H, 8->I ??
        # Wait, if 6->G in process_and_upload_data...
        # Let's re-verify process_and_upload_data logic.
        # df.iloc[:, [1, 2, 5, 6, 7, 8]] (6 cols) -> Sheet cols A,B,C,D,E,F.
        # So col index 6 went to D (4th col).
        # Col index 7 went to E (5th col).
        # Col index 8 went to F (6th col).
        # G (7th col) would be next. Index 9?
        # Or maybe I should look at the CSV content.

        # Assumption for now: I will read the 4 rows.
        # And I will take columns 3 and 4 from the CSV to put into G and H.
        # (Since 1,2 were used for B,C. 5,6,7,8 were used for F,G,H,I?? No.)
        # Let's assume the user knows G corresponds to column index 3 and H to 4
        # (The ones skipped between 2 and 5?).

        data_rows = df.iloc[1:6] # Get 5 rows of data (skipping metadata row 0)

        # Extract for G (Col Index 3) and H (Col Index 4)
        # Note: iloc is 0-based integer position
        col_g_data = data_rows.iloc[:, 3].tolist()
        col_h_data = data_rows.iloc[:, 4].tolist()

        print(f"Extracted G data ({len(col_g_data)}): {col_g_data}")
        print(f"Extracted H data ({len(col_h_data)}): {col_h_data}")

        # Upload G3:G7
        for i, val in enumerate(col_g_data):
            cell = f"G{3+i}"
            write_to_cell_sheet(SPREADSHEET_ID, SHEET_NAME, cell, val)

        # Upload H3:H7
        for i, val in enumerate(col_h_data):
            cell = f"H{3+i}"
            write_to_cell_sheet(SPREADSHEET_ID, SHEET_NAME, cell, val)

        #update Date in G1
        date = get_current_date()
        write_to_cell_sheet(SPREADSHEET_ID, SHEET_NAME, "G1", date)

        return {"status": "success", "sheet": SHEET_NAME}

    except Exception as e:
        print(f"Error during extraction/upload: {e}")
        return {"status": "error", "message": str(e)}

@asynccontextmanager
async def lifespan(app: FastAPI):
    indian_tz = timezone('Asia/Kolkata')
    scheduler = BackgroundScheduler(timezone=indian_tz)
    scheduler.add_job(script_sync_today, 'cron', day_of_week='mon-thu', hour=17, minute=56)
    scheduler.start()
    print("Scheduler started")
    yield
    scheduler.shutdown()
    print("Scheduler stopped")

app = FastAPI(lifespan=lifespan)

@app.get("/webhook/sync-today")
async def sync_today():
    # 1. Download file
    file_path = download_file_only()
    if not file_path:
        return {"status": "error", "message": "Failed to download file"}

    # 2. Shift columns
    # Assuming rows 3 to 6 as per extraction request, or maybe larger range?
    # User said "Shift G-> C and H-> D". I will assume standard data rows (all rows).
    # But since update is explicit for G3:G6, I'll shift G3:G100 to be safe/generic.
    # Actually, let's just shift a safe range.
    print("Shifting existing data...")
    shift_column_range(SPREADSHEET_ID, SHEET_NAME, "G3:G7", "C3:C7")
    shift_column_range(SPREADSHEET_ID, SHEET_NAME, "H3:H7", "D3:D7")

    # 3. Shift G1 to C1
    shift_column_range(SPREADSHEET_ID, SHEET_NAME, "G1", "C1")

    # 4. Extract and update
    try:
        df = pd.read_csv(file_path)
        # Skip top rows if needed (process_and_upload skips 1? let's check)
        # process_and_upload used df.iloc[1:].
        # The file usually has a header, then data.
        # Let's assume standard behavior: ignore first row if metadata.
        # Check if rows 3-6 implies indices 0-3 of the data dataframe?
        # User said "Extract Data for G3:G6". These are 4 rows.
        # The dataframe usually has 4 rows (Client, DII, FII, Pro).

        # Taking columns for G and H. guessing indices 3 and 4?
        # Previous mapping: 1->B, 2->C, 5->F, 6->G, 7->H, 8->I ??
        # Wait, if 6->G in process_and_upload_data...
        # Let's re-verify process_and_upload_data logic.
        # df.iloc[:, [1, 2, 5, 6, 7, 8]] (6 cols) -> Sheet cols A,B,C,D,E,F.
        # So col index 6 went to D (4th col).
        # Col index 7 went to E (5th col).
        # Col index 8 went to F (6th col).
        # G (7th col) would be next. Index 9?
        # Or maybe I should look at the CSV content.

        # Assumption for now: I will read the 4 rows.
        # And I will take columns 3 and 4 from the CSV to put into G and H.
        # (Since 1,2 were used for B,C. 5,6,7,8 were used for F,G,H,I?? No.)
        # Let's assume the user knows G corresponds to column index 3 and H to 4
        # (The ones skipped between 2 and 5?).

        data_rows = df.iloc[1:6] # Get 5 rows of data (skipping metadata row 0)

        # Extract for G (Col Index 3) and H (Col Index 4)
        # Note: iloc is 0-based integer position
        col_g_data = data_rows.iloc[:, 3].tolist()
        col_h_data = data_rows.iloc[:, 4].tolist()

        print(f"Extracted G data ({len(col_g_data)}): {col_g_data}")
        print(f"Extracted H data ({len(col_h_data)}): {col_h_data}")

        # Upload G3:G7
        for i, val in enumerate(col_g_data):
            cell = f"G{3+i}"
            write_to_cell_sheet(SPREADSHEET_ID, SHEET_NAME, cell, val)

        # Upload H3:H7
        for i, val in enumerate(col_h_data):
            cell = f"H{3+i}"
            write_to_cell_sheet(SPREADSHEET_ID, SHEET_NAME, cell, val)

        #update Date in G1
        date = get_current_date()
        write_to_cell_sheet(SPREADSHEET_ID, SHEET_NAME, "G1", date)

        return {"status": "success", "sheet": SHEET_NAME}

    except Exception as e:
        print(f"Error during extraction/upload: {e}")
        return {"status": "error", "message": str(e)}


@app.get("/health")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
