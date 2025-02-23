from loading.load_files import upload_files, get_config
from pathlib import Path
if __name__ == "__main__":
    
    path = f'{Path(__file__).resolve().parent}\\data\\10_k_filings'
    upload_files(path, f'{get_config("database")}.{get_config("schema")}.{get_config("stage")}',num_tickers=10,download=False, upload=True)
    print("Download process completed.")