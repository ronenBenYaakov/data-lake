import hashlib
from urllib.parse import urlparse
import gzip
from bs4 import BeautifulSoup

def sanitize_filename(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.replace("/", "_").strip("_") or "index"
    return f"{parsed.netloc}_{path}.html.gz"

def sha256_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def html_to_text(html_bytes: bytes) -> str:
    soup = BeautifulSoup(html_bytes, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)
