import requests

# URL of your FastAPI server
SERVER_URL = "http://localhost:7860/upload"

# List of URLs to upload
urls_to_upload = [
    'https://en.wikipedia.org/wiki/Blairstown,_New_Jersey'
]

# Prepare payload as required by your API
payload = {"urls": urls_to_upload}

try:
    response = requests.post(SERVER_URL, json=payload)
    response.raise_for_status()  # Raise error if status code is not 2xx
    data = response.json()
    print("Uploaded files:")
    for f in data.get("uploaded", []):
        print(f)
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
