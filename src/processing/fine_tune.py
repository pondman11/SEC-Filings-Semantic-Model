import utils.snowflake_utils as snowflake_utils
from src.loading.load_sec_filings import SECEdgarUploader

def fine_tune_models(conn,schema,target_model="llama3.1-70b",base_model="mistral-7b"): 

    loader = SECEdgarUploader("10_k_filings",conn)
    loader.load_10k_filings(amount=10)
    config = snowflake_utils.get_schema_config(schema)
    prompt = snowflake_utils.load_config("prompts/prompts.yml")["categorization"]
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
    
    cur.execute(f"""CREATE OR REPLACE TEMP TABLE FINE_TUNING_DATA AS (
                    SELECT TOP 500
                    {prompt} || CHUNK || '.' as prompt,
                    TRIM(snowflake.cortex.COMPLETE (
                        '{target_model}',
                        {prompt} || CHUNK || '.'
                        ), '\n') AS completion
                    FROM CHUNKS
                    ORDER BY RANDOM()
                    );""")  

    cur.execute(f"""CREATE OR REPLACE TEMP TABLE FINE_TUNE_TRAIN AS (
                    SELECT prompt, completion
                    FROM FINE_TUNING_DATA
                    QUALIFY ROW_NUMBER() OVER (ORDER BY PROMPT) <=  COUNT(completion) OVER (ORDER BY 1) * 0.7
                    );""")  
    
    cur.execute(f"""CREATE OR REPLACE TEMP TABLE FINE_TUNE_VALIDATION AS (
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