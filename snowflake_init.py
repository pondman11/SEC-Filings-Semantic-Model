import snowflake.connector
import os
from src.utils.snowflake_utils import load_config, load_snowflake_config
from src.loading.load_sec_filings import upload_files

def setup_snowflake_database(conn):
    """Execute the Snowflake database setup script."""
    sql_file_path = "sql/create_snowflake_db.sql"

    if not os.path.exists(sql_file_path):
        print(f"SQL script not found: {sql_file_path}")
        return

    with open(sql_file_path, "r") as f:
        sql_script = f.read()

    cur = conn.cursor()
    for query in sql_script.split(";"):
        if query.strip():
            cur.execute(query)
    cur.close()

    print("✅ Snowflake database setup completed successfully!")

def extract_all_udfs(config):
    """Extracts all UDFs and their details from the YAML config."""
    udfs = {}
    try:
        for udf_name, udf_data in config["udfs"].items():
            udfs[udf_name] = {
                "source": udf_data["source"],
                "declaration": udf_data["declaration"]
            }
    except KeyError:
        return None

    return udfs

def stage_and_declare_udfs(conn,schema):
    config = load_config(schema)
    udfs = extract_all_udfs(config)
    cur = conn.cursor()
    for key in udfs.keys():
        cur.execute(f"PUT file://./src/utils/snowflake_udfs/{udfs[key]['source']} @{config['database']}.{config['schema']}.UDFS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;")
        print(f"✅ Successfully loaded UDF {key} to stage {config['database']}.{config['schema']}.UDFS...\n")
        cur.execute(f"USE SCHEMA {config['database']}.{config['schema']};")
        cur.execute(udfs[key]['declaration'])
        print(f"✅ Successfully declared UDF {key}...\n")
    
    cur.close()

def fine_tune_models(conn,schema): 

    #upload_files("data/10_k_filings",num_tickers=10,download=True,upload=True)
    config = load_config(schema)
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
                        'llama3.1-70b',
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
                    'mistral-7b',
                    'SELECT prompt, completion FROM FINE_TUNE_TRAIN',
                    'SELECT prompt, completion FROM FINE_TUNE_VALIDATION',
                    '{"max_epochs": 5}');""")
    cur.close()



def main():
    """Main function to initialize the Snowflake database."""
    # Load credentials
    snowflake_config = load_snowflake_config()

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=snowflake_config["user"],
        authenticator=snowflake_config["authenticator"],
        account=snowflake_config["account"],
        warehouse=snowflake_config["warehouse"],
        role=snowflake_config["role"],
    )

    # Run database setup
    #setup_snowflake_database(conn)
    #stage_and_declare_udfs(conn,"raw")
    fine_tune_models(conn,"raw")

    # Close connection
    conn.close()
    print("✅ Snowflake initialization script completed.")

if __name__ == "__main__":
    main()
