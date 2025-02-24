import pandas as pd
from langchain.textsplitter import RecursiveCharacterTextSplitter
from snowflake.snowpark.files import SnowflakeFile

def process(pdf_text: str):

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 1024, 
            chunk_overlap  = 256, #This let's text have some form of overlap. Useful for keeping chunks contextual
            length_function = len
        )
    
        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        
        yield from df.itertuples(index=False, name=None)


def read_text(stage_path): 
    with SnowflakeFile.open(stage_path, 'r') as f:
        content = f.read()
    
    return content