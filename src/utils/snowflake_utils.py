import yaml
import os
import snowflake.connector

def load_config(env):
    """Load Snowflake credentials from YAML configuration file."""
    config_path = os.path.join(os.path.dirname(__file__), "..","..","config", "snowflake_config.yml")
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config[env]

def check_snowflake_database():
    """Check if the Snowflake database exists."""
    config = load_snowflake_config()
    
    conn = snowflake.connector.connect(
        user=config["user"],
        password=config["password"],
        account=config["account"],
        warehouse=config["warehouse"],
        role=config["role"],
    )

    cur = conn.cursor()
    cur.execute("SHOW DATABASES LIKE 'SEC_FILINGS_DB'")

    database_exists = cur.fetchone() is not None
    cur.close()
    conn.close()

    return database_exists

def get_schema_config(schema):
    return load_config(schema)

def load_snowflake_config(): 
    return load_config("snowflake")

def get_stage(schema): 
    config = get_schema_config(schema)
    return f'{config["database"]}.{config["schema"]}.{config["stage"]}'

def load_to_stage(schema, files): 
    stage = get_stage(schema)
    try: 
        conn = snowflake.connector.connect(**load_snowflake_config())
        curr = conn.cursor()
        for file in files:
            print(f"Uploading {file} to stage {stage}...\\n") 
            curr.execute(f"PUT file://{file}/*.txt @{stage} AUTO_COMPRESS=TRUE")
    finally:
        curr.close()
        conn.close()
        print(f"Successfully uploaded {len(files)} files to stage {stage}...\n")


def clear_stage(schema):
    stage = get_stage(schema)
    try: 
        conn = snowflake.connector.connect(**load_snowflake_config())
        curr = conn.cursor()
        print(f"Removing files from stage {stage}...\n")
        curr.execute(f"REMOVE @{stage}")
    finally:
        curr.close()
        conn.close()
        print(f"Successfully removed files from stage {stage}...\n")


def load_udfs(schema):
    config = load_config(schema)
    stage = f'{config["database"]}.{config["schema"]}.UDFS'
    try: 
        conn = snowflake.connector.connect(**load_snowflake_config())
        curr = conn.cursor()
        print(f"Loading UDFs to stage {stage}...\n")
        curr.execute(f'PUT file://src/utils/snowflake_udfs/{config["udfs"]["source"]} @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE')
    finally:
        curr.close()
        conn.close()
        print(f"Successfully loaded UDFs to stage {stage}...\n")

def create_udfs(schema,function): 
    config = load_config(schema)
    stage = f'{config["database"]}.{config["schema"]}.UDFS'
    try: 
        conn = snowflake.connector.connect(**load_snowflake_config())
        curr = conn.cursor()
        file_exists = curr.execute(f"LIST @{stage} LIKE {config['udfs']['source']}") 

        if file_exists is None:
            load_udfs(stage)
        
        curr.execute(f'{config["udfs"][function]["declaration"]}')

    finally: 
        curr.close()
        conn.close()
        print(f"Successfully created UDF {function}...\n")

  