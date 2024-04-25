import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType

def load_config(path):
    """ Load configuration settings from a YAML file. """
    with open(path, 'r') as file:
        return yaml.safe_load(file)

def load_schema_from_python_file(path):
    """ Dynamically import a schema defined in a Python file. """
    import importlib.util
    spec = importlib.util.spec_from_file_location("module.name", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.schema

spark = SparkSession.builder \
    .appName("Kafka to Bronze Streaming Job") \
    .getOrCreate()

config = load_config("/dbfs/path/to/your/config.yaml")

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config['kafka']['bootstrap_servers']) \
    .option("subscribe", config['kafka']['topic']) \
    .option("startingOffsets", config['kafka']['starting_offsets']) \
    .load()

# Process the value column from Kafka
value_df = df.selectExpr("CAST(value AS STRING)")

if config['schema']['use_schema_registry']:
    from pyspark.avro.functions import from_avro
    schema_str = spark.read.format("avro").load(config['schema']['schema_registry_url']).schema.json()
    value_df = value_df.select(from_avro(col("value"), schema_str).alias("data")).select("data.*")
else:
    schema = load_schema_from_python_file(config['schema']['schema_fallback_python_file'])
    value_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Add the 'Ingested_At' timestamp column
value_df = value_df.withColumn("Ingested_At", current_timestamp())

# Write stream to Delta Lake
query = value_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", config['streaming']['checkpoint_location']) \
    .option("path", config['streaming']['output_path']) \
    .trigger(processingTime=config['streaming']['trigger_processing_time']) \
    .start()

# Set or update table properties (tags)
tag_set_sql = ", ".join([f"'{k}' = '{v}'" for k, v in config['streaming']['tags'].items()])
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {config['streaming']['database_name']}.{config['streaming']['table_name']}
    USING DELTA
    LOCATION '{config['streaming']['output_path']}'
    TBLPROPERTIES (delta.enableChangeDataFeed = {str(config['streaming']['enable_cdf']).lower()}, {tag_set_sql})
""")

query.awaitTermination()
