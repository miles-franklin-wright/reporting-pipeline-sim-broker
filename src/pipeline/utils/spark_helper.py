# pipeline/utils/spark_helper.py
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark(spark_config: dict = None) -> SparkSession:
    """
    Return a SparkSession configured for Delta Lake on Windows (and local filesystems).
    - Exports HADOOP_HOME so winutils.exe is found.
    - Honors any user overrides (extensions, catalog, nativeio_disable, custom jars).
    """
    cfg = spark_config or {}

    # 1) Set HADOOP_HOME from winutils_path or home_dir
    if winux := cfg.get("winutils_path"):
        hdir = cfg.get("home_dir") or os.path.dirname(winux)
        os.environ["HADOOP_HOME"] = hdir

    builder = SparkSession.builder.appName("reporting-service")

    # 2) Apply user overrides for extensions & catalog
    if cfg.get("extensions"):
        builder = builder.config("spark.sql.extensions", cfg["extensions"])
    if cfg.get("catalog"):
        builder = builder.config("spark.sql.catalog.spark_catalog", cfg["catalog"])

    # 3) Accept extra jars, if provided; otherwise let Delta’s helper handle jars
    if jars := cfg.get("jars_packages"):
        builder = builder.config("spark.jars.packages", jars)

    # 4) Optionally disable native IO on Windows
    if cfg.get("nativeio_disable"):
        builder = builder.config("spark.hadoop.io.nativeio.disable", "true")

    # 5) Wrap with Delta’s helper – this injects the right jars & extensions
    builder = configure_spark_with_delta_pip(builder)

    # 6) Return the SparkSession
    return builder.getOrCreate()
