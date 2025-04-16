import os
import shutil
import tempfile
import pytest
from pyspark.sql import SparkSession
from pipeline.ingest.bronze_writer import ingest_jsonl, ingest_csv, create_spark_session

@pytest.fixture(scope="session")
def spark():
    spark = create_spark_session()
    yield spark
    spark.stop()

@pytest.fixture
def temp_dir():
    dirpath = tempfile.mkdtemp()
    yield dirpath
    shutil.rmtree(dirpath)

def test_ingest_jsonl(spark, temp_dir):
    # Create a temporary JSONL file with sample data
    json_file = os.path.join(temp_dir, "test.jsonl")
    with open(json_file, "w") as f:
        f.write('{"col1": "value1", "col2": 10}\n')
        f.write('{"col1": "value2", "col2": 20}\n')
    
    output_path = os.path.join(temp_dir, "output_json")
    rows_ingested = ingest_jsonl(spark, json_file, output_path)
    
    df = spark.read.format("delta").load(output_path)
    assert df.count() == rows_ingested
    assert df.count() == 2

def test_ingest_csv(spark, temp_dir):
    # Create a temporary CSV file with sample data
    csv_file = os.path.join(temp_dir, "test.csv")
    with open(csv_file, "w") as f:
        f.write("col1,col2\n")
        f.write("value1,10\n")
        f.write("value2,20\n")
    
    output_path = os.path.join(temp_dir, "output_csv")
    rows_ingested = ingest_csv(spark, csv_file, output_path)
    
    df = spark.read.format("delta").load(output_path)
    assert df.count() == rows_ingested
    assert df.count() == 2
