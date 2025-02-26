from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd

class chunk_text: 

    def process(self,pdf_text: str):
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 1024, 
            chunk_overlap  = 256, #This let's text have some form of overlap. Useful for keeping chunks contextual
            length_function = len
        )

        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        
        yield from df.itertuples(index=False, name=None)
