from src.loading.load_sec_filings import upload_files
import os
from src.utils.prompts import Chat

if __name__ == "__main__":
    #path = os.path.join(os.path.dirname(__file__),"data","10_k_filings")
    #upload_files(path,num_tickers=10,download=True, upload=True)
    #rag_preprocessing.main()
    chat = Chat()
    print(chat.prompt('test'))