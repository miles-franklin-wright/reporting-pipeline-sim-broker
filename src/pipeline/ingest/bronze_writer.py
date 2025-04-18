import os
import sys
import yaml
import logging

from pipeline.utils.spark_helper import get_spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# ── Logging setup ────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Config loader ────────────────────────────────────────────────────────────────
def load_config(config_path: str) -> dict:
    """
    Load YAML config, exit on failure.
    """
    try:
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f)
        logger.info(f"Loaded config from {config_path}")
        return cfg
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

# ── Ingestion functions ─────────────────────────────────────────────────────────
def ingest_jsonl(
    spark: SparkSession,
    file_path: str,
    output_path: str,
    fmt: str = "delta",
    skip_write: bool = False
) -> int:
    logger.info(f"Reading JSONL → {file_path}")
    df = spark.read.json(file_path)
    count = df.count()
    logger.info(f"Rows read: {count}")

    if not skip_write:
        logger.info(f"Writing {fmt.upper()} to → {output_path}")
        df.write.mode("overwrite").format(fmt).save(output_path)
        logger.info(f"Wrote {fmt.upper()} successfully to {output_path}")
    else:
        logger.info("skip_write=True; skipping write phase")

    return count


def ingest_csv(
    spark: SparkSession,
    file_path: str,
    output_path: str,
    fmt: str = "delta",
    skip_write: bool = False
) -> int:
    logger.info(f"Reading CSV → {file_path}")
    df = spark.read.option("header", True).csv(file_path)
    count = df.count()
    logger.info(f"Rows read: {count}")

    if not skip_write:
        logger.info(f"Writing {fmt.upper()} to → {output_path}")
        df.write.mode("overwrite").format(fmt).save(output_path)
        logger.info(f"Wrote {fmt.upper()} successfully to {output_path}")
    else:
        logger.info("skip_write=True; skipping write phase")

    return count


def ingest_dimensions(
    spark: SparkSession,
    file_path: str,
    output_path: str,
    skip_write: bool = False
) -> int:
    if not os.path.exists(file_path):
        logger.warning(f"{file_path} missing; creating placeholder")
        schema = StructType([StructField("note", StringType(), True)])
        df = spark.createDataFrame([("Placeholder: dimensions not found",)], schema)
        count = df.count()
        if not skip_write:
            df.write.mode("overwrite").format("delta").save(output_path)
        return count
    return ingest_csv(spark, file_path, output_path, fmt="delta", skip_write=skip_write)

# ── Main flow ───────────────────────────────────────────────────────────────────
def main(config_path: str):
    cfg = load_config(config_path)
    spark = get_spark()
    run_id = cfg.get("run_id", "default")

    tasks = {
        "orders":   ("orders.jsonl", ingest_jsonl),
        "trades":   ("trades.jsonl", ingest_jsonl),
        "positions": ("positions.csv", ingest_csv),
        "pnl":      ("pnl.csv", ingest_csv),
    }

    summary = {}
    for key, (fname, fn) in tasks.items():
        inp = os.path.join(cfg["input_dir"], fname)
        out = os.path.join(cfg["lake_root"], "bronze", key, run_id)
        summary[key] = fn(spark, inp, out, fmt="delta", skip_write=False)

    logger.info("Ingestion complete:")
    for k, v in summary.items():
        logger.info(f"  {k}: {v} rows")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python bronze_writer.py <config.yml>")
        sys.exit(1)
    main(sys.argv[1])
