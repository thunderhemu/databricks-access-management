import yaml
from pyspark.sql import SparkSession

def load_config(path):
    """Load configuration settings from a YAML file."""
    with open(path, 'r') as file:
        return yaml.safe_load(file)

spark = SparkSession.builder.appName("Data Load with Flexible Query Inputs").getOrCreate()

def read_query_from_file(file_path):
    """Read an SQL query from a specified file."""
    with open(file_path, 'r') as file:
        return file.read().strip()

def fetch_data():
    """Fetch data from a relational database based on the configuration."""
    if config['query']['use_query']:
        query = config['query']['query_text'] or read_query_from_file(config['query']['query_file'])
    else:
        query = f"(SELECT *, CURRENT_TIMESTAMP as Ingested_At FROM {config['database']['table']}) AS subquery"

    return spark.read.format("jdbc") \
        .option("url", config['database']['jdbc_url']) \
        .option("dbtable", query) \
        .option("user", config['database']['user']) \
        .option("password", config['database']['password']) \
        .option("driver", config['database']['driver']) \
        .load()

def save_data(df):
    """Save the DataFrame to Delta Lake at the specified path, with options for partitioning, overwriting, and optionally creating a table in Unity Catalog."""
    path = config['table']['delta_path']
    write_mode = "overwrite" if config['table']['overwrite'] else "append"
    df.write.format("delta").mode(write_mode).partitionBy(*config['table']['partition_by']).save(path)
    
    if config['table']['create_table']:
        spark.sql(f"CREATE TABLE IF NOT EXISTS default.{config['table']['unity_catalog_table']} USING DELTA LOCATION '{path}'")
        # Apply tags as table properties
        for key, value in config['table']['tags'].items():
            spark.sql(f"ALTER TABLE default.{config['table']['unity_catalog_table']} SET TBLPROPERTIES ('{key}' = '{value}')")

    if config['table']['retain_versions'] > 0:
        spark.sql(f"VACUUM default.{config['table']['unity_catalog_table']} RETAIN {config['table']['retain_versions']} HOURS")

    print("Data written and table managed with tags in Unity Catalog.")

config = load_config("/dbfs/path/to/your/config.yaml")
df = fetch_data()
save_data(df)
