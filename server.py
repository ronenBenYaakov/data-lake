from fastapi import FastAPI, HTTPException, Request, UploadFile
from pydantic import BaseModel, HttpUrl
from typing import List
from pipeline import upload_html_to_s3
from processing import process_raw
from config import s3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import asyncio
from metadata import add_file, mark_processed


app = FastAPI()
executor = ThreadPoolExecutor(max_workers=5)

class URLItem(BaseModel):
    urls: List[HttpUrl]

@app.post("/upload")
async def upload_urls(payload: URLItem):
    url_strings = [str(url) for url in payload.urls]
    raw_keys = upload_html_to_s3(url_strings)

    for key, url in zip(raw_keys, url_strings):
        add_file(file_name=key.split("/")[-1], s3_key=key, bucket="raw", source_url=url)

    loop = asyncio.get_running_loop()
    tasks = [loop.run_in_executor(executor, process_and_mark, key) for key in raw_keys]
    await asyncio.gather(*tasks)
    return {"raw": raw_keys}

def process_and_mark(raw_key):
    process_raw(raw_key)
    mark_processed(raw_key.split("/")[-1])


@app.post("/upload-file")
async def upload_file(file: UploadFile, tags: List[str] = None, notes: str = ""):
    raw_key = f"raw/files/{datetime.utcnow().strftime('%Y/%m/%d')}/{file.filename}"
    content = await file.read()

    content_type = file.content_type or "application/octet-stream"

    s3.put_object(
        Bucket="raw",
        Key=raw_key,
        Body=content,
        ContentType=content_type,
        Metadata={"uploaded_at": datetime.utcnow().isoformat()}
    )

    add_file(
        file_name=file.filename,
        s3_key=raw_key,
        bucket="raw",
        file_size=len(content),
        mime_type=content_type,
        tags=tags,
        notes=notes
    )

    return {"raw_key": raw_key}


@app.post("/minio-notify")
async def minio_notify(request: Request):
    data = await request.json()
    loop = asyncio.get_running_loop()

    for record in data.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        if bucket == "raw":
            asyncio.create_task(loop.run_in_executor(executor, process_raw, key))

    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7860)
