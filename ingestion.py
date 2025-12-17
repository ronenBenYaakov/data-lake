from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import gzip
from datetime import datetime
from config import s3
from utils import sanitize_filename, sha256_hash
from prefect import flow, task

def create_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429,500,502,503,504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_and_upload(url, bucket_name="raw"):
    session = create_session()
    headers = {"User-Agent": "DataLakeBot/1.0"}
    date_prefix = datetime.utcnow().strftime("%Y/%m/%d")
    resp = session.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    compressed = gzip.compress(resp.content)
    file_hash = sha256_hash(resp.content)
    key = f"raw/html/{date_prefix}/{sanitize_filename(url)}"
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=compressed,
        ContentType="text/html",
        ContentEncoding="gzip",
        Metadata={
            "source_url": url,
            "retrieved_at": datetime.utcnow().isoformat(),
            "sha256": file_hash
        }
    )
    return key

def upload_html_to_s3(urls, bucket_name="raw"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/114.0.0.0 Safari/537.36"
    }
    date_prefix = datetime.utcnow().strftime("%Y/%m/%d")
    uploaded_keys = []

    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            compressed = gzip.compress(resp.content)
            file_hash = sha256_hash(resp.content)
            key = f"raw/html/{date_prefix}/{sanitize_filename(url)}"
            s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=compressed,
                ContentType="text/html",
                ContentEncoding="gzip",
                Metadata={
                    "source_url": url,
                    "retrieved_at": datetime.utcnow().isoformat(),
                    "sha256": file_hash
                }
            )
            uploaded_keys.append(key)
        except Exception as e:
            print(f"Failed {url}: {e}")

    return uploaded_keys


@task
def fetch_and_upload_url(url):
    return upload_html_to_s3([url])

@flow
def scheduled_ingestion(urls):
    results = fetch_and_upload_url.map(urls)
    return results

