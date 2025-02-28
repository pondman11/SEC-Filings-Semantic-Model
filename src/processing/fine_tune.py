import utils.snowflake_utils as snowflake_utils

def fine_tune_models(conn,schema,target_model="llama3.1-70b",base_model="mistral-7b"): 

    #upload_files("data/10_k_filings",num_tickers=10,download=True,upload=True)
    config = snowflake_utils.get_schema_config(schema)
    cur = conn.cursor()
    cur.execute(f"USE SCHEMA {config['database']}.{config['schema']};")
    cur.execute(f"""CREATE OR REPLACE TEMP TABLE URLS AS (
                    select 
                    build_scoped_file_url(@{config['database']}.{config['schema']}."{config['stage']}", relative_path) as scoped_file_url,
                    from 
                    directory(@{config['database']}.{config['schema']}."{config['stage']}")
);""")
    cur.execute(f"""CREATE OR REPLACE TEMP TABLE CHUNKS AS (
                    SELECT 
                    func.chunk as CHUNK
                FROM URLS,
                TABLE(chunk_text(TO_VARCHAR(read_file(URLS.scoped_file_url)))) as func
                );
    """)
    prompt_1 = """Suppose you are analyzing company 10-K filings. 
    Given the context in the following chunk of text summarize the chunk in a single word. 
    The word should come from one of the following categories: 
        -markets
        -equity
        -debt
        -personnel
        -industry
        -product
        -litigation
        -economic
        -risk
        -growth
        -acquisition
        -merger
        -supply chain
        -operations 
    When the chunk contains context relevant to multiple categories, summarize based on the category most descriptive of the text. 
    If the summary does not fit into one of the categories, provide a blank response. 
    In your response do not include anything other than the word or empty response. 
    Here is the chunk: \n\n"""

    cur.execute(f"""CREATE OR REPLACE TEMP TABLE FINE_TUNING_DATA AS (
                    SELECT TOP 500
                    {prompt_1} || CHUNK || '.' as prompt,
                    TRIM(snowflake.cortex.COMPLETE (
                        '{target_model}',
                        {prompt_1} || CHUNK || '.'
                        ), '\n') AS completion
                    FROM CHUNKS
                    ORDER BY RANDOM()
                    );""")  

    cur.execute(f"""CREATE OR REPLACE TABLE FINE_TUNE_TRAIN AS (
                    SELECT prompt, completion
                    FROM FINE_TUNING_DATA
                    QUALIFY ROW_NUMBER() OVER (ORDER BY PROMPT) <=  COUNT(completion) OVER (ORDER BY 1) * 0.7
                    );""")  
    
    cur.execute(f"""CREATE OR REPLACE TABLE FINE_TUNE_VALIDATION AS (
                    SELECT prompt, completion
                    FROM FINE_TUNING_DATA
                    QUALIFY ROW_NUMBER() OVER (ORDER BY PROMPT) >  COUNT(completion) OVER (ORDER BY 1) * 0.7
                    );""")
    
    cur.execute(f"""SELECT SNOWFLAKE.CORTEX.FINETUNE(
                    'CREATE',
                    '{config['database']}.{config['schema']}.sec_10_k_labeler',
                    '{base_model}',
                    'SELECT prompt, completion FROM FINE_TUNE_TRAIN',
                    'SELECT prompt, completion FROM FINE_TUNE_VALIDATION',
                    '{"max_epochs": 5}');""")
    cur.close()