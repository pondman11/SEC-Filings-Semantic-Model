import pandas as pd
from sec_edgar_downloader import Downloader
import json
import snowflake.connector as sf
from pathlib import Path
from bs4 import BeautifulSoup
import os
import re

def get_config(component="connection",config_file='config.json') -> dict:
    script_dir = Path(__file__).resolve().parent.parent
    with open(f'{script_dir}\{config_file}', 'r') as file:
        config = json.load(file)
    return config.get(component, {})


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
    names = df["Security"].tolist()
    sectors = df["GICS Sector"].tolist()
    cleaned_tickers = [t.replace(".", "-") for t in tickers]
    cleaned_names = [re.sub(r'[.\s!@#$%^&*(),\-_=+[\]{};:"\'<>?/|\\`~]', '', re.sub(r'\(.*?\)', '', n)) for n in names]
    
    return cleaned_tickers, cleaned_names, sectors


def download_10k_filings(tickers, names, num_tickers, download_path, amount=1, ):
    
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
            create_text_file(text, date, ticker, names[i], html_path)
            os.remove(f'{html_path}\\primary-document.html')
        except Exception as e:
            print(f"Error downloading filings for {ticker}: {e}")

def get_leaf_folder(base_path):
    
    path = Path(base_path)
    
    for folder in path.rglob('*'):
        if folder.is_dir() and not any(sub.is_dir() for sub in folder.iterdir()):
            return str(folder)

def create_text_file(text, date, ticker, company_name,path):
    with open(f'{path}\\{ticker}_{company_name}_{date}.txt', 'w', encoding='utf-8') as file:
        file.write(text)

def clean_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    date = soup.find('title').get_text().split('-')[-1]
    text = soup.get_text()
    idx = text.find('Table of Contents')

    return date, text[idx:]

def upload_files(path,stage_name, num_tickers = 1,download = False, upload=False):

    
    dir = Path(f'{path}\\sec-edgar-filings')
    tickers, names, sectors = get_sp500_tickers()
    if download: 
        download_10k_filings(tickers, names, num_tickers, path, amount=1)

    print(dir)
    if upload:
        try: 
            conn = sf.connect(**get_config())
            cur = conn.cursor()
            file_paths = [str(subdir) for subdir in dir.iterdir() if subdir.is_dir()]
            for fp in file_paths:
                ticker = fp.split('\\')[-1]
                leaf = get_leaf_folder(fp)
                p = leaf.replace('\\','/')
                put_sql = f'PUT file://{p}/*.txt @{stage_name}/{ticker} AUTO_COMPRESS=FALSE'
                cur.execute(put_sql)
        finally: 
            cur.close()
            conn.close()
            print("Upload process completed.")