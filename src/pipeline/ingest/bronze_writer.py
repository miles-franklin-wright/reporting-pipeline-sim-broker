import os
import sys
import yaml
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from delta import configure_spark_with_delta_pip

# ── Logging setup ────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Config loader ────────────────────────────────────────────────────────────────
def load_config(config_path: str) -> dict:
    try:
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f)
        logger.info(f"Loaded config from {config_path}")
        return cfg
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

# ── SparkSession factory with Delta ──────────────────────────────────────────────
def create_spark_session(app_name="ReportingService", master="local[*]") -> SparkSession:
    builder = (
        SparkSession.builder
            .appName(app_name)
            .master(master)
            # 1) Delta SQL extension
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            # 2) Delta catalog plugin
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # 3) Pull Delta core JAR
            .config("spark.jars.packages", "io.delta:delta-core_2.12:3.1.0")
            # 4) Disable native IO on Windows to avoid UnsatisfiedLinkError
            .config("spark.hadoop.io.nativeio.disable", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

# ── Ingestion functions ─────────────────────────────────────────────────────────
def ingest_jsonl(spark: SparkSession, file_path: str, output_path: str) -> int:
    logger.info(f"Reading JSONL → {file_path}")
    df = spark.read.json(file_path)
    count = df.count()
    logger.info(f"Rows read: {count}")
    df.write.mode("overwrite").format("delta").save(output_path)
    logger.info(f"Wrote Delta to {output_path}")
    return count

def ingest_csv(spark: SparkSession, file_path: str, output_path: str) -> int:
    logger.info(f"Reading CSV → {file_path}")
    df = spark.read.option("header", True).csv(file_path)
    count = df.count()
    logger.info(f"Rows read: {count}")
    df.write.mode("overwrite").format("delta").save(output_path)
    logger.info(f"Wrote Delta to {output_path}")
    return count

def ingest_dimensions(spark: SparkSession, file_path: str, output_path: str) -> int:
    if not os.path.exists(file_path):
        logger.warning(f"{file_path} missing; creating placeholder")
        schema = StructType([StructField("note", StringType(), True)])
        df = spark.createDataFrame(
            [("Placeholder: dimensions not found",)], schema
        )
        count = df.count()
        df.write.mode("overwrite").format("delta").save(output_path)
        return count
    return ingest_csv(spark, file_path, output_path)

# ── Main flow ───────────────────────────────────────────────────────────────────
def main(config_path: str):
    cfg = load_config(config_path)
    spark = create_spark_session()
    run_id = cfg.get("run_id", "default")

    tasks = {
        "orders":   ("orders.jsonl", ingest_jsonl),
        "trades":   ("trades.jsonl", ingest_jsonl),
        "dimensions": ("dimensions.csv", ingest_dimensions),
    }

    summary = {}
    for key, (fname, fn) in tasks.items():
        inp = os.path.join(cfg["input_dir"], fname)
        out = os.path.join(cfg["lake_root"], "bronze", key, run_id)
        summary[key] = fn(spark, inp, out)

    logger.info("Ingestion complete:")
    for k, v in summary.items():
        logger.info(f"  {k}: {v} rows")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python bronze_writer.py <config.yml>")
        sys.exit(1)
    main(sys.argv[1])
