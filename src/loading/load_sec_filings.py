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

    def __init__(self,file_type,conn = None): 
        self.conn = conn if conn != None else connect()
        self.file_path = os.path.join(os.path.dirname(__file__),"..","..","data",file_type)
        self.dl = Downloader("Continuus", "mlake@continuus.ai",download_folder=self.file_path)
        
    def __download_filing(self,cik,filing_type):
            try:
                print(f'Downloading 10-K filing(s) for {cik}...')
                self.dl.get(
                    form=filing_type,
                    ticker_or_cik=cik,
                    limit=1,
                    download_details=True
                )
                self.__replace_html_file(cik,filing_type)
                print(f"Successfully downloaded {filing_type} filing for {cik}.\n")
            except Exception as e:
                print(f"Error downloading {filing_type} filing for {cik}: {e}")

    def __replace_html_file(self,cik):
        html_path = get_leaf_folder(f'{self.file_path}\\sec-edgar-filings\\{cik}')
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

    def __upload_filing(self,filing_type,cik):

        dir = Path(f'{self.file_path}\\{cik}\\{filing_type}\\sec-edgar-filings')
        clear_stage("raw")
        files = [get_leaf_folder(str(subdir)).replace("\\","/") for subdir in dir.iterdir() if subdir.is_dir()]
        load_to_stage("raw",files)
        print("Upload process completed.")

    def __load_filings(self,filing_type,amount = 10):
        ciks = get_CIKs()
        keys = list(ciks.keys())[:amount]
        for key in keys:
            self.__download_filing(filing_type,amount)
            self.__upload_filing(filing_type,ciks[key]["cik"])

    def load_10k_filnings(self,amount = 10):
        self.__load_filings("10-K",amount)