import requests
from urllib.parse import urlparse
from datetime import datetime
from config import s3

def sanitize_filename(url):
    parsed = urlparse(url)
    path = parsed.path.replace("/", "_").strip("_") or "index"
    return f"{parsed.netloc}_{path}.html"

def upload_html_to_s3(urls, bucket_name="raw"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/114.0.0.0 Safari/537.36"
    }
    date_prefix = datetime.utcnow().strftime("%Y/%m/%d")
    uploaded_files = []

    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            key = f"{date_prefix}/{sanitize_filename(url)}"
            s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=resp.content,
                ContentType="text/html",
                Metadata={
                    "source_url": url,
                    "retrieved_at": datetime.utcnow().isoformat()
                }
            )
            print(f"Uploaded {url} → s3://{bucket_name}/{key}")
            uploaded_files.append(f"s3://{bucket_name}/{key}")
        except requests.RequestException as e:
            print(f"Fetch failed {url}: {e}")
        except Exception as e:
            print(f"Upload failed {url}: {e}")
    
    return uploaded_files
