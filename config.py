import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
import os

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin123"
)

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
