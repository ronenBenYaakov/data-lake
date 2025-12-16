# server.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
from typing import List
from scrape import upload_html_to_s3

app = FastAPI()

class URLItem(BaseModel):
    urls: List[HttpUrl]

@app.post("/upload")
def upload_urls(payload: URLItem):
    # convert HttpUrl objects to strings
    url_strings = [str(url) for url in payload.urls]
    
    uploaded_files = upload_html_to_s3(url_strings)
    if not uploaded_files:
        raise HTTPException(status_code=400, detail="No files uploaded.")
    return {"uploaded": uploaded_files}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7860)
