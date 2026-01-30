import requests
import time

def trigger_download():
    url = "http://127.0.0.1:8080/webhook/start-download"
    try:
        print(f"Sending POST request to {url}...")
        response = requests.post(url)
        print(f"Response Status: {response.status_code}")
        print(f"Response Body: {response.json()}")
    except Exception as e:
        print(f"Error triggering webhook: {e}")

if __name__ == "__main__":
    # Wait a bit for server to start if running immediately after
    time.sleep(2) 
    trigger_download()
