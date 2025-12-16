from prefect import flow, task
import dask.dataframe as dd
import pandas as pd
import requests
from bs4 import BeautifulSoup
from config import SERPER_API_KEY

@task
def search_web(query, num_results=5):
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    payload = {"q": query, "num": num_results}
    resp = requests.post("https://google.serper.dev/search", json=payload, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    rows = []
    for res in data.get("organic", [])[:num_results]:
        link = res.get("link")
        snippet = res.get("snippet", "")
        title = res.get("title", "")
        if link:
            rows.append({"url": link, "title": title, "snippet": snippet})
    return rows

@task
def fetch_full_page(row):
    url = row["url"]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/117.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        content = soup.get_text(separator="\n", strip=True)
        row["content"] = content
    except Exception:
        row["content"] = row.get("snippet", "Failed to fetch content")
    return row

@task
def load_dataset(rows, output_path):
    df = pd.DataFrame(rows)
    ddf = dd.from_pandas(df, npartitions=4)
    ddf.to_parquet(output_path, engine="pyarrow", write_index=False)
    return output_path

@flow
def web_search_etl(query, output_file):
    search_results = search_web(query)
    rows = [fetch_full_page(row) for row in search_results]
    path = load_dataset(rows, output_file)
    print(f"ETL complete! Dataset saved at: {path}")

if __name__ == "__main__":
    web_search_etl("US law questions", "usa_laws.parquet")
