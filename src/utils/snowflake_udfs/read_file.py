from snowflake.snowpark.files import SnowflakeFile


def read(stage_path): 

    with SnowflakeFile.open(stage_path, 'r') as f:
        content = f.read()
    
    return content