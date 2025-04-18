# tests/unit/test_ingest.py
import os
import json
import pytest
from pathlib import Path

from pipeline.utils.spark_helper import get_spark
from pipeline.ingest.bronze_writer import ingest_jsonl, ingest_csv

@pytest.fixture(scope="module")
def spark():
    spark = get_spark()
    yield spark
    spark.stop()

def write_jsonl(tmp_path, records):
    p = tmp_path / "data.jsonl"
    with p.open("w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    return str(p)

def write_csv(tmp_path, header, rows):
    p = tmp_path / "data.csv"
    with p.open("w") as f:
        f.write(",".join(header) + "\n")
        for r in rows:
            f.write(",".join(map(str, r)) + "\n")
    return str(p)

def test_ingest_jsonl_counts_and_skips_write(spark, tmp_path):
    # prepare 2 records
    records = [{"a":1}, {"a":2}]
    jsonl = write_jsonl(tmp_path, records)
    outdir = tmp_path / "out_json"
    # skip_write=True should count but not write
    count = ingest_jsonl(spark, jsonl, str(outdir), fmt="parquet", skip_write=True)
    assert count == 2
    assert not outdir.exists()

def test_ingest_csv_counts_and_skips_write(spark, tmp_path):
    header = ["x","y"]
    rows = [[10,100],[20,200]]
    csvf = write_csv(tmp_path, header, rows)
    outdir = tmp_path / "out_csv"
    count = ingest_csv(spark, csvf, str(outdir), fmt="parquet", skip_write=True)
    assert count == 2
    assert not outdir.exists()
