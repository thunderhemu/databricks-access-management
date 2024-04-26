import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as max_, col
from delta.tables import DeltaTable
from datetime import datetime
import pytz

def load_config(config_path: str) -> dict:
    """ Load configuration from a YAML file. """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def get_last_processed_value(spark, config):
    """ Fetch the last processed value of the incremental column from the Delta table if it exists; otherwise, use a default value. """
    try:
        if DeltaTable.isDeltaTable(spark, config['delta']['bronze_path']):
            delta_table = DeltaTable.forPath(spark, config['delta']['bronze_path'])
            last_value = delta_table.toDF().select(max_(col(config['database']['last_updated_column']))).collect()[0][0]
            return last_value if last_value is not None else config['database']['last_run_time']
        else:
            return config['database']['last_run_time']
    except Exception as e:
        print("Error accessing Delta table or it does not exist: ", e)
        return config['database']['last_run_time']

def read_data_from_rdbms(spark, config):
    """ Read data from RDBMS using JDBC with optional incremental query. """
    db_config = config['database']
    query = db_config.get('query_text')
    
    # Load query from file if specified
    if db_config.get('query_file'):
        with open(db_config['query_file'], 'r') as file:
            query = file.read().strip()
    
    # Apply incremental logic if specified
    if db_config.get('incremental_load', False):
        last_value = get_last_processed_value(spark, config)
        query += f" WHERE {db_config['last_updated_column']} > '{last_value}'"

    # If neither query text nor file is provided, use the table name directly
    if not query:
        query = f"SELECT * FROM {db_config['dbtable']}"

    return spark.read \
        .format("jdbc") \
        .option("url", db_config['url']) \
        .option("query", query) \
        .option("user", db_config['user']) \
        .option("password", db_config['password']) \
        .option("driver", db_config['driver']) \
        .load()

def write_data_to_delta(spark, dataframe, config):
    """ Write data to Delta Lake with optional partitioning and schema evolution. """
    delta_config = config['delta']
    dataframe.write \
        .format("delta") \
        .mode(delta_config['load_type']) \
        .partitionBy(*delta_config['partition_columns']) \
        .option("mergeSchema", str(delta_config.get('enable_schema_evolution', False)).lower()) \
        .save(delta_config['bronze_path'])

def update_last_run_time(config_path, new_run_time):
    """ Update the last run time in the configuration file after a successful load. """
    config = load_config(config_path)
    config['database']['last_run_time'] = new_run_time.isoformat()
    with open(config_path, 'w') as file:
        yaml.safe_dump(config, file)

# Example Usage
if __name__ == "__main__":
    config_path = "path_to_config.yaml"
    config = load_config(config_path)
    spark = SparkSession.builder.appName("Data Loader").getOrCreate()

    df = read_data_from_rdbms(spark, config)
    write_data_to_delta(spark, df, config)
    update_last_run_time(config_path, datetime.now(pytz.utc))
