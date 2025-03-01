import pandas as pd
from sec_edgar_downloader import Downloader
import snowflake.connector as sf
from pathlib import Path
from bs4 import BeautifulSoup
from src.utils.snowflake_utils import load_to_stage, clear_stage, connect, get_schema_config
from src.utils.web_retrieval_utils import get_CIKs
from src.utils.file_utils import get_leaf_folder, create_text_file
import os

class SECEdgarUploader: 

    def __init__(self,filing_type,conn = None): 
        self.conn = conn if conn != None else connect()
        self.file_path = os.path.join(os.path.dirname(__file__),"..","..","data",filing_type)
        self.dl = Downloader("Continuus", "mlake@continuus.ai",download_folder=self.file_path)
        
    def __download_filing(self,path,ticker,filing_type):
            try:
                print(f'Downloading 10-K filing(s) for {ticker}...')
                self.dl.get(
                    form=filing_type,
                    ticker_or_cik=ticker,
                    limit=1,
                    download_details=True
                )
                self.__replace_html_file(path,ticker,filing_type)
                print(f"Successfully downloaded {filing_type} filing for {ticker}.\n")
            except Exception as e:
                print(f"Error downloading {filing_type} filing for {ticker}: {e}")

    def __replace_html_file(self,path,ticker,filing_type):
        html_path = get_leaf_folder(f'{path}\\sec-edgar-filings\\{ticker}\\{filing_type}')
        with open(f'{html_path}\primary-document.html', 'r') as file:
            html = file.read()
        date, text = self.__clean_html(html)
        os.remove(f'{html_path}\\full-submission.txt')
        create_text_file(f'{html_path}\{ticker}_{date}.txt',text)
        os.remove(f'{html_path}\\primary-document.html')

    def __clean_html(self,html):
        soup = BeautifulSoup(html, 'html.parser')
        date = soup.find('title').get_text().split('-')[-1]
        text = soup.get_text()
        idx = text.find('Table of Contents')

        return date, text[idx:]

    def __upload_filing(self,path,ticker,filing_type):
        schema = get_schema_config("raw")
        clear_stage(self.conn,schema,"10_K_FILINGS")
        dir = get_leaf_folder(f'{path}\\sec-edgar-filings\\{ticker}\\{filing_type}')
        load_to_stage(self.conn,schema,"10_K_FILINGS",dir,"txt")
        print("Upload process completed.")

    def __load_filings(self,path,filing_type,amount = 10):
        ciks = get_CIKs()
        keys = list(ciks.keys())[:amount]
        for key in keys:
            self.__download_filing(path,ciks[key]["ticker"],filing_type)
            self.__upload_filing(path,ciks[key]["ticker"],filing_type)

    def load_10k_filings(self,amount = 10):
        self.__load_filings(self.file_path,"10-K",amount)

    def close(self): 
        self.conn.close()