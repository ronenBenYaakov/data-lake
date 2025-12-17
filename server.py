from fastapi import FastAPI, HTTPException, Request, UploadFile
from pydantic import BaseModel, HttpUrl
from typing import List
from pipeline import upload_html_to_s3
from processing import process_raw
from config import s3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import asyncio


app = FastAPI()
executor = ThreadPoolExecutor(max_workers=5)

class URLItem(BaseModel):
    urls: List[HttpUrl]

@app.post("/upload")
async def upload_urls(payload: URLItem):
    url_strings = [str(url) for url in payload.urls]
    raw_keys = upload_html_to_s3(url_strings)

    if not raw_keys:
        raise HTTPException(status_code=400, detail="No files uploaded.")

    loop = asyncio.get_running_loop()
    tasks = [loop.run_in_executor(executor, process_raw, key) for key in raw_keys]
    processed_keys = await asyncio.gather(*tasks)

    return {"raw": raw_keys, "processed": processed_keys}

@app.post("/upload-file")
async def upload_file(file: UploadFile):
    raw_key = f"raw/files/{datetime.utcnow().strftime('%Y/%m/%d')}/{file.filename}"
    content = await file.read()

    s3.put_object(
        Bucket="raw",
        Key=raw_key,
        Body=content,
        ContentType=file.content_type,
        Metadata={"uploaded_at": datetime.utcnow().isoformat()}
    )

    loop = asyncio.get_running_loop()
    asyncio.create_task(loop.run_in_executor(executor, process_raw, raw_key))

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
