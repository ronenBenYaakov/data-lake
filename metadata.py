import json
from datetime import datetime
from config import s3

METADATA_BUCKET = "metadata"

def add_file(file_name, s3_key, bucket="raw", source_url=None, sha256=None,
             file_size=None, mime_type=None, tags=None, notes=None):
    metadata = {
        "file_name": file_name,
        "s3_key": s3_key,
        "bucket": bucket,
        "source_url": source_url,
        "uploaded_at": datetime.utcnow().isoformat(),
        "processed_at": None,
        "sha256": sha256,
        "status": "raw",
        "file_size": file_size,
        "mime_type": mime_type,
        "tags": tags or [],
        "notes": notes or ""
    }
    key = f"{bucket}/metadata/{file_name}.json"
    s3.put_object(Bucket=METADATA_BUCKET, Key=key,
                  Body=json.dumps(metadata).encode(),
                  ContentType="application/json")

def mark_processed(file_name, bucket="raw"):
    key = f"{bucket}/metadata/{file_name}.json"
    obj = s3.get_object(Bucket=METADATA_BUCKET, Key=key)
    metadata = json.loads(obj["Body"].read())
    metadata["status"] = "processed"
    metadata["processed_at"] = datetime.utcnow().isoformat()
    s3.put_object(Bucket=METADATA_BUCKET, Key=key,
                  Body=json.dumps(metadata).encode(),
                  ContentType="application/json")

def get_metadata(bucket="raw"):
    objs = s3.list_objects_v2(Bucket=METADATA_BUCKET, Prefix=f"{bucket}/metadata/")
    all_metadata = []
    for obj in objs.get("Contents", []):
        file_obj = s3.get_object(Bucket=METADATA_BUCKET, Key=obj["Key"])
        data = json.loads(file_obj["Body"].read())
        all_metadata.append(data)
    return all_metadata
