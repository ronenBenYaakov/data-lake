import gzip
from datetime import datetime
from config import s3
from utils import html_to_text

def process_raw(raw_key, raw_bucket="raw", processed_bucket="processed"):
    raw_obj = s3.get_object(Bucket=raw_bucket, Key=raw_key)
    html = gzip.decompress(raw_obj["Body"].read())
    text = html_to_text(html)
    date_prefix = datetime.utcnow().strftime("%Y/%m/%d")
    processed_key = raw_key.replace("raw/html/", f"processed/text/{date_prefix}/").replace(".html.gz", ".txt")
    s3.put_object(
        Bucket=processed_bucket,
        Key=processed_key,
        Body=text.encode("utf-8"),
        ContentType="text/plain",
        Metadata={"source_key": raw_key}
    )
    return processed_key
