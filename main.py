import os
import urllib.request
import urllib.error
from fastapi import FastAPI, BackgroundTasks
import uvicorn
from getdate import get_current_date

from sheet_service import process_and_upload_data

app = FastAPI()

def download_nse_file():
    date=get_current_date()
    # URL provided by user
    url = "https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_"+date+".csv"
    
    # Setup paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(base_dir, "Download")
    filename = os.path.basename(url)
    #print(filename)
    output_path = os.path.join(download_dir, filename)

    # Ensure download directory exists
    if not os.path.exists(download_dir):
        try:
            os.makedirs(download_dir)
            print(f"Created directory: {download_dir}")
        except OSError as e:
            print(f"Error creating directory {download_dir}: {e}")
            return

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
        
        # Upload to Google Sheet
        try:
            process_and_upload_data(output_path)
        except Exception as e:
            print(f"Failed to upload to Google Sheet: {e}")
            
    except Exception as e:
        print(f"Error downloading file: {e}")

@app.get("/webhook/start-download")
async def start_download(background_tasks: BackgroundTasks):
    background_tasks.add_task(download_nse_file)
    return {"status": "download started"}

@app.get("/health")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
