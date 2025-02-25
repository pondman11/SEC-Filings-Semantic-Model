def read_text(stage_path): 

    from snowflake.snowpark.files import SnowflakeFile
    with SnowflakeFile.open(stage_path, 'r') as f:
        content = f.read()
    
    return content