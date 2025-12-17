from prefect import flow, task
from ingestion import fetch_and_upload
from processing import process_raw
from ingestion import upload_html_to_s3

@flow
def raw_to_processed_pipeline(urls):
    raw_paths = upload_html_to_s3(urls)
    processed_keys = process_raw.map(raw_paths)
    return processed_keys