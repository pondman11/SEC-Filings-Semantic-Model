import src.utils.snowflake_utils as snowflake_utils
from src.loading.load_sec_filings import SECEdgarUploader

def fine_tune_models(conn,schema,target_model="llama3.1-70b",base_model="mistral-7b"): 

    #loader = SECEdgarUploader("10_k_filings",conn)
    #loader.load_10k_filings(amount=10)
    config = snowflake_utils.get_schema_config(schema)
    stage = snowflake_utils.get_stage_name(config,"10_K_FILINGS")
    prompt = snowflake_utils.load_config("prompts/prompts.yml")["categorization"]
    cur = conn.cursor()
    cur.execute(f"USE SCHEMA {config['database']}.{config['schema']};")
    cur.execute(f"ALTER STAGE {stage} REFRESH;")
    cur.execute(f"""CREATE OR REPLACE TEMP TABLE URLS AS (
                    select 
                    build_scoped_file_url(@{stage}, relative_path) as scoped_file_url,
                    from 
                    directory(@{stage})
    );""")
    cur.execute(f"""CREATE OR REPLACE TEMP TABLE CHUNKS AS (
                    SELECT 
                    func.chunk as CHUNK
                FROM URLS,
                TABLE(chunk_text(TO_VARCHAR(read_file(URLS.scoped_file_url)))) as func
                );
    """)
    
    print("Creating training data...\n")
    sql = """CREATE OR REPLACE TEMP TABLE FINE_TUNING_DATA AS (
                    SELECT TOP 500
                    %s || CHUNK || '.' as prompt,
                    TRIM(snowflake.cortex.COMPLETE (
                        %s,
                        %s || CHUNK || '.'
                        ), '\n') AS completion
                    FROM CHUNKS
                    ORDER BY RANDOM()
                    );"""
    cur.execute(sql,(prompt,target_model,prompt))  
    print("Splitting training data...\n")
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
    print("Executing fine-tuning...\n")
    cur.execute(f"""SELECT SNOWFLAKE.CORTEX.FINETUNE(
                    'CREATE',
                    'sec_10_k_labeler',
                    '{base_model}',
                    'SELECT prompt, completion FROM FINE_TUNE_TRAIN',
                    'SELECT prompt, completion FROM FINE_TUNE_VALIDATION');""")
    
    cur.close()