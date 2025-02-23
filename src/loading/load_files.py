import pandas as pd
from sec_edgar_downloader import Downloader
import snowflake.connector as sf
from pathlib import Path
from bs4 import BeautifulSoup
from src.utils.snowflake_utils import load_snowflake_config, get_stage
from src.utils.file_utils import get_leaf_folder, create_text_file
import os


def get_sp500_tickers():
    import requests
    from bs4 import BeautifulSoup

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    
    response = requests.get(url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "constituents"})
    df = pd.read_html(str(table))[0]
    tickers = df["Symbol"].tolist()
    cleaned_tickers = [t.replace(".", "-") for t in tickers]
    
    return cleaned_tickers

def download_10k_filings(tickers, num_tickers, download_path, amount=1):
    
    dl = Downloader("Continuus", "mlake@continuus.ai",download_folder=download_path)
    
    for i, ticker in enumerate(tickers[:num_tickers]):
        try:
            print(f"Downloading {amount} 10-K filing(s) for {ticker}...")
            dl.get(
                form="10-K",
                ticker_or_cik=ticker,
                limit=amount,
                download_details=True
            )

            html_path = get_leaf_folder(f'{download_path}\\sec-edgar-filings\\{ticker}')
            with open(f'{html_path}\primary-document.html', 'r') as file:
                html = file.read()
            date, text = clean_html(html)
            os.remove(f'{html_path}\\full-submission.txt')
            create_text_file(f'{html_path}\{ticker}_{date}.txt',text)
            os.remove(f'{html_path}\\primary-document.html')
        except Exception as e:
            print(f"Error downloading filings for {ticker}: {e}")

def clean_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    date = soup.find('title').get_text().split('-')[-1]
    text = soup.get_text()
    idx = text.find('Table of Contents')

    return date, text[idx:]

def upload_files(path,num_tickers = 1,download = False, upload=False):

    
    dir = Path(f'{path}\\sec-edgar-filings')
    stage = get_stage("raw")
    tickers = get_sp500_tickers()
    if download: 
        download_10k_filings(tickers,num_tickers, path, amount=1)

    print(dir)
    if upload:
        try: 
            conn = sf.connect(**load_snowflake_config())
            cur = conn.cursor()
            file_paths = [str(subdir) for subdir in dir.iterdir() if subdir.is_dir()]
            for fp in file_paths:
                ticker = fp.split('\\')[-1]
                leaf = get_leaf_folder(fp)
                p = leaf.replace('\\','/')
                put_sql = f'PUT file://{p}/*.txt @{stage}/{ticker} AUTO_COMPRESS=FALSE'
                cur.execute(put_sql)
        finally: 
            cur.close()
            conn.close()
            print("Upload process completed.")