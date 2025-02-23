from src.utils.snowflake_utils import create_udfs, load_snowflake_config
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.cortex import complete 

def call_model():
    config = load_snowflake_config()
    session = Session.builder.configs(config).create()
    df = session.create_dataframe([("mistral-7b","Describe yourself")],schema=["model","prompt"])
    out= complete("mistral-7b",df["prompt"],session=session)
    df = df.with_column("completion",out)
    print(df.show())
    session.close()