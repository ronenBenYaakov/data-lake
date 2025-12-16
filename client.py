import requests

API_URL = "http://localhost:7860/upload"  # or replace with server IP if remote

urls = [
    "https://en.wikipedia.org/wiki/Law",
    "https://en.wikipedia.org/wiki/Artificial_intelligence"
]

payload = {"urls": urls}

try:
    response = requests.post(API_URL, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    print("Uploaded files:")
    for f in data.get("uploaded", []):
        print(f)
except requests.RequestException as e:
    print("Error uploading URLs:", e)
