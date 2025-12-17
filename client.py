# client.py
import requests

API_URL = "http://localhost:7860/upload-file"

def upload_file(file_path, tags=None, notes=""):
    tags = tags or []
    with open(file_path, "rb") as f:
        files = {"file": (file_path.split("/")[-1], f)}
        data = {"notes": notes}
        # for tags, FastAPI expects list input like tags=tag1&tags=tag2
        params = [("tags", t) for t in tags]
        response = requests.post(API_URL, files=files, data=data, params=params)
    if response.status_code == 200:
        print("Upload successful!")
        print(response.json())
    else:
        print("Upload failed!")
        print(response.status_code, response.text)

if __name__ == "__main__":
    upload_file(
        "missing_info_questions_1.csv",
        tags=["legal", "csv"],
        notes="Test upload of HTML file"
    )
