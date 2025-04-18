# src/pipeline/utils/spark_helper.py
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os


def get_spark(spark_config: dict = None) -> SparkSession:
    """
    Return a SparkSession configured for Delta Lake, honoring user-specified spark_config.

    spark_config keys:
      - extensions: Spark SQL extensions to enable (e.g. DeltaSparkSessionExtension)
      - catalog: Spark catalog implementation (e.g. DeltaCatalog)
      - jars_packages: comma-delimited jars to include
      - nativeio_disable: bool to disable native IO (Windows fix)
    """
    builder = (
        SparkSession.builder
        .appName("reporting-service")
    )

    # Apply config overrides if provided
    if spark_config:
        if spark_config.get("extensions"):
            builder = builder.config(
                "spark.sql.extensions",
                spark_config["extensions"]
            )
        if spark_config.get("catalog"):
            builder = builder.config(
                "spark.sql.catalog.spark_catalog",
                spark_config["catalog"]
            )
        if spark_config.get("jars_packages"):
            builder = builder.config(
                "spark.jars.packages",
                spark_config["jars_packages"]
            )
        if spark_config.get("nativeio_disable"):
            builder = builder.config(
                "spark.hadoop.io.nativeio.disable",
                "true"
            )

    # Use Java-only log store to avoid native IO issues on Windows
    builder = builder.config(
        "spark.delta.logStore.class",
        "org.apache.spark.sql.delta.storage.FileStreamLogStore"
    )

    # Pull in Delta Lake JARs via pip-based integration
    builder = configure_spark_with_delta_pip(builder)

    # Optionally set HADOOP_HOME for winutils
    hadoop_cfg = spark_config or {}
    if hadoop_cfg.get("winutils_path"):
        os.environ["HADOOP_HOME"] = hadoop_cfg.get("home_dir", os.path.dirname(hadoop_cfg["winutils_path"]))

    return builder.getOrCreate()
