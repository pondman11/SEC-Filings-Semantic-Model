import pandas as pd
from sec_edgar_downloader import Downloader
import snowflake.connector as sf
from pathlib import Path
from bs4 import BeautifulSoup
from src.utils.snowflake_utils import load_to_stage, clear_stage, connect
from src.utils.web_retrieval_utils import get_CIKs
from src.utils.file_utils import get_leaf_folder, create_text_file
import os

class SECEdgarUploader: 

    def __init__(self,filing_type,conn = None): 
        self.conn = conn if conn != None else connect()
        self.file_path = os.path.join(os.path.dirname(__file__),"..","..","data",filing_type)
        self.dl = Downloader("Continuus", "mlake@continuus.ai",download_folder=self.file_path)
        
    def __download_filing(self,path,cik,filing_type):
            try:
                print(f'Downloading 10-K filing(s) for {cik}...')
                self.dl.get(
                    form=filing_type,
                    ticker_or_cik=cik,
                    limit=1,
                    download_details=True
                )
                self.__replace_html_file(path,cik,filing_type)
                print(f"Successfully downloaded {filing_type} filing for {cik}.\n")
            except Exception as e:
                print(f"Error downloading {filing_type} filing for {cik}: {e}")

    def __replace_html_file(self,path,cik,filing_type):
        html_path = get_leaf_folder(f'{path}\\sec-edgar-filings\\{cik}\\{filing_type}')
        with open(f'{html_path}\primary-document.html', 'r') as file:
            html = file.read()
        date, text = self.__clean_html(html)
        os.remove(f'{html_path}\\full-submission.txt')
        create_text_file(f'{html_path}\{cik}_{date}.txt',text)
        os.remove(f'{html_path}\\primary-document.html')

    def __clean_html(self,html):
        soup = BeautifulSoup(html, 'html.parser')
        date = soup.find('title').get_text().split('-')[-1]
        text = soup.get_text()
        idx = text.find('Table of Contents')

        return date, text[idx:]

    def __upload_filing(self,path,cik,filing_type):

        dir = Path(f'{path}\\{cik}\\{filing_type}\\sec-edgar-filings')
        clear_stage(self.conn,"raw","10_K_FILINGS")
        files = [get_leaf_folder(str(subdir)).replace("\\","/") for subdir in dir.iterdir() if subdir.is_dir()]
        load_to_stage(self.conn,"raw",files)
        print("Upload process completed.")

    def __load_filings(self,path,filing_type,amount = 10):
        ciks = get_CIKs()
        keys = list(ciks.keys())[:amount]
        for key in keys:
            self.__download_filing(path,ciks[key]["cik_str"],filing_type)
            self.__upload_filing(path,ciks[key]["cik_str"],filing_type)

    def load_10k_filings(self,amount = 10):
        path = os.path.join(self.file_path,"10_k_filings")
        self.__load_filings(path,"10-K",amount)

    def close(self): 
        self.conn.close()