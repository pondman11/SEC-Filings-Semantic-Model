from src.loading.load_files import upload_files
from src.processing.rag_preprocessing import call_model, main
import os

if __name__ == "__main__":
    path = os.path.join(os.path.dirname(__file__),"data","10_k_filings")
    upload_files(path,num_tickers=10,download=True, upload=True)
    main()