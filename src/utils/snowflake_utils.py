import yaml
import os
import snowflake.connector

def load_config(file_name):
    """Load Snowflake credentials from YAML configuration file."""
    config_path = os.path.join(os.path.dirname(__file__), "..","..","config", file_name)
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

def connect(): 
    config = get_snowflake_config()
    conn = snowflake.connector.connect(
        user=config["user"],
        authenticator=config["authenticator"],
        account=config["account"],
        warehouse=config["warehouse"],
        database=config["database"],
        role=config["role"]
    )
    return conn

def check_snowflake_database(conn):
    """Check if the Snowflake database exists."""
    
    cur = conn.cursor()
    cur.execute("SHOW DATABASES LIKE 'SEC_FILINGS_DB'")

    database_exists = cur.fetchone() is not None
    cur.close()

    return database_exists

def get_schema_config(schema):
    config = load_config("schema_config.yml")
    return config[schema]

def get_snowflake_config(): 
    config = load_config("snowflake_connection_config.yml")
    return config["snowflake"]

def get_stage_name(schema,stage): 
    return f'{schema["database"]}.{schema["schema"]}."{stage}"'

def load_to_stage(conn,schema,stage,dir,ext): 
    stage = get_stage_name(schema,stage)
    try: 
        curr = conn.cursor()
        print(f"Uploading {dir} to stage {stage}...\\n") 
        curr.execute(f"PUT file://{dir}/*.{ext} @{stage} AUTO_COMPRESS=FALSE")
    finally:
        curr.close()
        print(f"Successfully uploaded files to stage {stage}...\n")


def clear_stage(conn,schema,stage):
    stage = get_stage_name(schema,stage)
    try: 
        curr = conn.cursor()
        print(f"Removing files from stage {stage}...\n")
        curr.execute(f"REMOVE @{stage}")
    finally:
        curr.close()
        print(f"Successfully removed files from stage {stage}...\n")


def stage_udf_file(conn,schema_config,udf):
    stage = get_stage_name(schema_config,"UDFS")
    try: 
        curr = conn.cursor()
        print(f"Loading UDFs to stage {stage}...\n")
        curr.execute(f'PUT file://src/utils/snowflake_udfs/{schema_config["udfs"][udf]["source"]} @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE')
    finally:
        curr.close()
        print(f"Successfully loaded UDFs to stage {stage}...\n")

def create_udf(conn,schema_config,udf,stage="UDFS"): 
    stage = get_stage_name(schema_config,stage)
    try: 
        curr = conn.cursor()
        stage_udf_file(conn,schema_config,udf)
        print(f"Declaring UDF {udf}...\n")
        curr.execute(f'USE SCHEMA {schema_config["database"]}.{schema_config["schema"]};')
        curr.execute(f'{schema_config["udfs"][udf]["declaration"]}')

    finally: 
        curr.close()
        print(f"Successfully created UDF {udf}...\n")

def create_udfs(conn,schema_config):
    for udf in schema_config["udfs"].keys():
        create_udf(conn,schema_config,udf)

def create_database(conn,schema_config): 
    try: 
        curr = conn.cursor()
        curr.execute(f"CREATE DATABASE IF NOT EXISTS {schema_config['database']}")
    finally: 
        curr.close()
        print(f"Successfully created database {schema_config['database']}...\n") 

def create_schema(conn,schema_config):
    try: 
        curr = conn.cursor()
        curr.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_config['database']}.{schema_config['schema']}")
    finally: 
        curr.close()
        print(f"Successfully created schema {schema_config['schema']}...\n")

def create_stage(conn,schema_config,stage):
    try: 
        curr = conn.cursor()
        sql = [f'CREATE OR REPLACE STAGE {schema_config["database"]}.{schema_config["schema"]}."{stage}"']
        if schema_config["stages"][stage]["requires_sse"]: 
            sql.append(" ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')")
        if schema_config["stages"][stage]["is_directory"]: 
            sql.append(" DIRECTORY = ( ENABLE = TRUE )")
        sql.append(";")
        sql = "".join(sql)
        curr.execute(sql)

    finally: 
        curr.close()
        print(f"Successfully created stage {stage}...\n")

def create_stages(conn,schema_config): 
    for stage in schema_config["stages"].keys(): 
        create_stage(conn,schema_config,stage)

def create_table(conn,schema_config,table):
    try:
        curr = conn.cursor()
        sql = [f'CREATE OR REPLACE TABLE {schema_config["database"]}.{schema_config["schema"]}."{table}" (\n']
        columns = schema_config['tables'][table].keys()
        for i, column in enumerate(columns): 
            sql.append(f"{column} {schema_config['tables'][table][column]['TYPE']}{',' if i < len(columns) - 1 else ''}\n")
        sql.append(");")
        sql = "".join(sql)
        curr.execute(sql)
    finally: 
        curr.close()
        print(f"Successfully created table {table}...\n")

def create_tables(conn,schema_config): 
    for table in schema_config["tables"].keys(): 
        create_table(conn,schema_config,table)

def run_sql_file(conn,sql_file):
    path = os.path.join(os.path.dirname(__file__), "..","..","sql",sql_file) 
    with open(path,"r") as file: 
        sql = file.read()
    try: 
        statements = sql.split(";")
        curr = conn.cursor()
        for statement in statements: 
            curr.execute(statement)
        
    finally: 
        curr.close()

def run_sql_files(conn,schema_config):
    for sql_file in schema_config["sql_files"]: 
        run_sql_file(conn,sql_file)

def configure_environment(conn,schema_config): 
    create_database(conn,schema_config)
    create_schema(conn,schema_config)
    create_stages(conn,schema_config)
    create_tables(conn,schema_config)
    create_udfs(conn,schema_config)

def configure_environments(conn): 
    schemas = load_config("schema_config.yml")
    for schema in schemas.keys(): 
        configure_environment(conn,schemas[schema])
