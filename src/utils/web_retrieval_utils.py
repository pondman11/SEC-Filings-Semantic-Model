from bs4 import BeautifulSoup
import requests
import pandas as pd

def get_sp500_tickers():

        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"id": "constituents"})
        df = pd.read_html(str(table))[0]
        tickers = df["Symbol"].tolist()
        cleaned_tickers = [t.replace(".", "-") for t in tickers]
        
        return cleaned_tickers

def get_CIKs(): 
        url = "https://www.sec.gov/files/company_tickers.json"
        # Provide a descriptive User-Agent with contact info
        headers = {
        "User-Agent": "MyAppName/1.0 (mike.lake23@yahoo.com)"
        }

        response = requests.get(url,headers=headers)
        response.raise_for_status()
        data = response.json()
        return data