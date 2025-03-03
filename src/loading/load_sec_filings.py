import pandas as pd
from sec_edgar_downloader import Downloader
import snowflake.connector as sf
from pathlib import Path
from bs4 import BeautifulSoup
from src.utils.snowflake_utils import load_to_stage, refresh_stage, exectute_task, connect, get_schema_config
from src.utils.web_retrieval_utils import get_CIKs
from src.utils.file_utils import get_leaf_folder, create_text_file, delete_dir
import os
from datetime import datetime
import re
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
                raise e

    def __replace_html_file(self,path,ticker,filing_type):
        html_path = get_leaf_folder(f'{path}\\sec-edgar-filings\\{ticker}\\{filing_type}')
        with open(f'{html_path}\primary-document.html', 'r') as file:
            html = file.read()
        text, date = self.__clean_html(html)
        os.remove(f'{html_path}\\full-submission.txt')
        create_text_file(f'{html_path}\{ticker}_{date}.txt',text)
        os.remove(f'{html_path}\\primary-document.html')

    def __clean_html(self,html):
        soup = BeautifulSoup(html, 'html.parser')

        # Extract the filing date from the title
        title_text = soup.find('title')
        date = title_text.get_text().split('-')[-1] if title_text else "Unknown Date"

        # Remove scripts, styles, and metadata
        for tag in soup(["script", "style", "meta", "head"]):
            tag.decompose()

        # Extract text content
        text = soup.get_text(separator=" ")

        # Remove SEC disclaimers and boilerplate text
        boilerplate_patterns = [
            r"This document has been truncated", 
            r"This is an HTML version of an ASCII filing",
            r"Filed with the Securities and Exchange Commission",
            r"\bUNITED STATES SECURITIES AND EXCHANGE COMMISSION\b",
            r"\bWashington, D\.C\.\s*\d+\b",  # Matches SEC office locations
            r"Form\s+10-K",  # If "Form 10-K" appears multiple times, remove early occurrences
            r"Table of Contents\s*\n+",
        ]
        
        for pattern in boilerplate_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)

        # Locate the actual start of the filing content
        start_keywords = ["Table of Contents", "PART I", "Item 1."]
        start_index = None

        for keyword in start_keywords:
            match = re.search(rf"\b{keyword}\b", text, re.IGNORECASE)
            if match:
                start_index = match.start()
                break

        if start_index:
            text = text[start_index:]

        # Remove excessive whitespace and empty lines
        text = re.sub(r"\n\s*\n+", "\n\n", text).strip()

        return text, date



    def __upload_filing(self,path,ticker,filing_type):
        schema = get_schema_config("raw")
        dir = get_leaf_folder(f'{path}\\sec-edgar-filings\\{ticker}\\{filing_type}')
        formatted_date = datetime.now().strftime('%Y%m%d')
        stage_folder = f'{formatted_date}/{ticker}'
        load_to_stage(self.conn,schema,"10_K_FILINGS",dir,"txt",stage_folder)
        refresh_stage(self.conn,schema,"10_K_FILINGS")
        exectute_task(self.conn,schema,"PROCESS_CHUNKS")
        delete_dir(f'{path}\\sec-edgar-filings\\{ticker}')
        print("Upload process completed.")

    def __load_filings(self,path,filing_type,amount = None):
        ciks = get_CIKs()
        keys = list(ciks.keys())

        if amount != None: 
            keys = keys[:amount]
            
        for key in keys:
            try: 
                self.__download_filing(path,ciks[key]["ticker"],filing_type)
                self.__upload_filing(path,ciks[key]["ticker"],filing_type)
            except Exception as e:
                print(f"Error loading {filing_type} filing for {ciks[key]['ticker']}: {e}")

    def load_10k_filings(self,amount = 10):
        self.__load_filings(self.file_path,"10-K",amount)

    def close(self): 
        self.conn.close()