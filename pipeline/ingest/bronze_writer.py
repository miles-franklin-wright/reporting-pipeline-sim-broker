import os
import sys
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(config_path: str) -> dict:
    """Load ingestion configuration from a YAML file."""
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        logger.info(f"Configuration loaded from {config_path}: {config}")
        return config
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        sys.exit(1)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("SimBroker Bronze Ingestion")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.home.dir", os.environ.get("HADOOP_HOME"))
        .getOrCreate()
    )
    logger.info("Spark session created successfully.")
    return spark



def ingest_jsonl(spark: SparkSession, file_path: str, output_path: str) -> int:
    """Read a JSONL file and write it to a Delta Lake folder."""
    logger.info(f"Reading JSONL file from: {file_path}")
    df = spark.read.json(file_path)
    row_count = df.count()
    logger.info(f"Ingested {row_count} rows from {file_path}")
    df.write.format("delta").mode("overwrite").save(output_path)
    logger.info(f"Data written to Delta Lake at: {output_path}")
    return row_count

def ingest_csv(spark: SparkSession, file_path: str, output_path: str) -> int:
    """Read a CSV file and write it to a Delta Lake folder."""
    logger.info(f"Reading CSV file from: {file_path}")
    df = spark.read.option("header", True).csv(file_path)
    row_count = df.count()
    logger.info(f"Ingested {row_count} rows from {file_path}")
    df.write.format("delta").mode("overwrite").save(output_path)
    logger.info(f"Data written to Delta Lake at: {output_path}")
    return row_count

def ingest_dimensions(spark: SparkSession, file_path: str, output_path: str) -> int:
    """Ingest dimensions data if available; otherwise, write a placeholder."""
    if not os.path.exists(file_path):
        # Log warning and create a placeholder Delta table
        logger.warning(f"{file_path} not found. Creating placeholder for dimensions data.")
        # Define a minimal schema; adjust as needed for your application.
        schema = StructType([
            StructField("note", StringType(), True)
        ])
        # Create a single row DataFrame with placeholder data
        placeholder_data = [("Placeholder: Dimensions data not available",)]
        df = spark.createDataFrame(placeholder_data, schema)
        row_count = df.count()
        df.write.format("delta").mode("overwrite").save(output_path)
        logger.info(f"Placeholder data written to Delta Lake at: {output_path}")
        return row_count
    else:
        return ingest_csv(spark, file_path, output_path)

def main(config_path: str):
    # Load configuration details from config.yml
    config = load_config(config_path)
    input_dir = config.get("input_dir")
    lake_root = config.get("lake_root")
    run_id = config.get("run_id", "default_run")

    # Initialize Spark session
    spark = create_spark_session()

    # Define file ingestion specifications (adjust filenames as needed)
    files_to_ingest = {
        "orders": {"filename": "orders.jsonl", "read_fn": ingest_jsonl},
        "trades": {"filename": "trades.jsonl", "read_fn": ingest_jsonl},
        # For dimensions, we use a separate function that handles missing file scenarios.
        "dimensions": {"filename": "dimensions.csv", "read_fn": ingest_dimensions}
    }

    ingestion_summary = {}

    # Process each file type
    for key, spec in files_to_ingest.items():
        file_path = os.path.join(input_dir, spec["filename"])
        # Construct output path as: lake_root/bronze/<file_type>/<run_id>
        output_path = os.path.join(lake_root, "bronze", key, run_id)
        row_count = spec["read_fn"](spark, file_path, output_path)
        ingestion_summary[key] = row_count

    logger.info("Ingestion Summary:")
    for key, count in ingestion_summary.items():
        logger.info(f"{key}: {count} rows ingested.")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python bronze_writer.py <path_to_config_yml>")
        sys.exit(1)
    config_file = sys.argv[1]
    main(config_file)
