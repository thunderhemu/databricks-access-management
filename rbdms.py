import yaml
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from delta.tables import *
from datetime import datetime
import pytz

def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def get_last_processed_value():
    """ Fetch the last processed value of the incremental column from the Delta table. """
    try:
        df = spark.read.format("delta").load(config['output']['path'])
        last_value = df.select(max_(col(config['load']['column']))).collect()[0][0]
        return last_value
    except Exception as e:
        print("Error reading from Delta table: ", e)
        # Return a default or initial value if the table is empty or does not exist
        return config['load']['last_value']

def validate_config(config):
    expected_keys = {
        'database': ['url', 'user', 'password', 'driver', 'dbtable', 'incremental_load', 'last_updated_column', 'last_run_time'],
        'delta': ['bronze_path', 'file_format', 'load_type', 'use_partitioning', 'partition_columns', 'enable_schema_evolution'],
        'unity_catalog': ['enable', 'database', 'table_name']
    }
    
    missing = {}
    for section, keys in expected_keys.items():
        if section not in config:
            missing[section] = keys
        else:
            missing_keys = [key for key in keys if key not in config[section]]
            if missing_keys:
                missing[section] = missing_keys
    
    if missing:
        raise ValueError(f"Missing configuration sections/keys: {missing}")

def read_data_from_rdbms(spark, db_config):
    if db_config.get('incremental_load'):
        last_run_time = db_config.get('last_run_time')
        last_updated_column = db_config.get('last_updated_column')
        query = f"SELECT * FROM {db_config['dbtable']} WHERE {last_updated_column} > '{last_run_time}'"
    else:
        query = f"SELECT * FROM {db_config['dbtable']}"

    return spark.read \
        .format("jdbc") \
        .option("url", db_config['url']) \
        .option("query", query) \
        .option("user", db_config['user']) \
        .option("password", db_config['password']) \
        .option("driver", db_config['driver']) \
        .load()

def validate_data(dataframe):
    if dataframe is None or dataframe.count() == 0:
        raise ValueError("Dataframe is empty or null.")
    # Additional data validations can be implemented here based on business rules

def write_data_to_delta(spark, dataframe, delta_config, unity_config):
    write_options = {
        "mode": delta_config['load_type'],
        "path": delta_config['bronze_path'],
        "format": delta_config['file_format']
    }
    if delta_config['use_partitioning'] and delta_config['partition_columns']:
        dataframe.write \
            .format(delta_config['file_format']) \
            .mode(delta_config['load_type']) \
            .partitionBy(delta_config['partition_columns']) \
            .option("mergeSchema", str(delta_config.get('enable_schema_evolution', False)).lower()) \
            .save()

    if unity_config['enable']:
        spark.sql(f"""
            CREATE OR REPLACE TABLE {unity_config['database']}.{unity_config['table_name']}
            USING DELTA
            LOCATION '{delta_config['bronze_path']}'
        """)

def update_last_run_time(config_path, new_run_time):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    config['database']['last_run_time'] = new_run_time
    with open(config_path, 'w') as file:
        yaml.safe_dump(config, file)
