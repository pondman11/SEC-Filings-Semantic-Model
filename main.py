from src.loading.load_sec_filings import SECEdgarUploader
from src.utils.prompts import Chat
from src.utils.web_retrieval_utils import get_CIKs

if __name__ == "__main__":
    file_loader = SECEdgarUploader("10_k_filings")
    file_loader.load_10k_filings(amount=1)
    file_loader.close()