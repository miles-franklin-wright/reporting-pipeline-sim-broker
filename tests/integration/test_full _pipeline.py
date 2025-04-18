# tests/integration/test_full_pipeline.py
import sys
import subprocess
from pathlib import Path
import pytest

def test_full_pipeline(tmp_path):
    # ── 1) Bootstrap directories ──────────────────────────────────────────────
    input_dir   = tmp_path / "input"
    lake_root   = tmp_path / "lake"
    reports_dir = tmp_path / "reports"
    for d in (input_dir, lake_root, reports_dir):
        d.mkdir()

    # ── 2) Write sample data ─────────────────────────────────────────────────
    # trades.jsonl
    (input_dir / "trades.jsonl").write_text(
        '{"trade_id":"T1","desk_id":"D1","symbol":"ABC","quantity":10,"price":100,"real_pnl_usd":50,"fee_usd":1,"trade_time":"2025-04-18T09:00:00"}\n'
    )
    # orders.jsonl
    (input_dir / "orders.jsonl").write_text(
        '{"order_id":"O1","desk_id":"D1","symbol":"ABC","quantity":5,"price":200}\n'
    )
    # positions.csv
    (input_dir / "positions.csv").write_text(
        "position_id,desk_id,symbol,quantity\nP1,D1,ABC,100\n"
    )
    # pnl.csv
    (input_dir / "pnl.csv").write_text(
        "pnl_id,desk_id,real_pnl_usd,fee_usd\nP1,D1,75,2.5\n"
    )

    # ── 3) Emit config.yml ───────────────────────────────────────────────────
    config = tmp_path / "config.yml"
    config.write_text(f"""
input_dir: {input_dir}
lake_root: {lake_root}
reports_dir: {reports_dir}
run_id: test_run
""")

    # ── 4) Invoke CLI ────────────────────────────────────────────────────────
    cmd = [
        sys.executable, "-m", "pipeline.cli", "ingest",
        "--config", str(config),
        "--html-report", "--print-summary"
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    # print(proc.stdout); print(proc.stderr)
    assert proc.returncode == 0, f"CLI failed: {proc.stderr}"

    # ── 5) Validate Bronze output ───────────────────────────────────────────
    # each subfolder under bronze should exist and contain files
    for artifact in ("trades", "orders", "positions", "pnl"):
        folder = lake_root / "bronze" / artifact
        assert folder.exists(), f"Missing bronze folder: {folder}"
        # should have at least one file (Delta parquet or log)
        assert any(folder.iterdir()), f"Bronze folder is empty: {folder}"

    # ── 6) Validate Gold output ─────────────────────────────────────────────
    gold_folder = lake_root / "gold" / "fact_pnl_daily"
    assert gold_folder.exists(), f"Missing gold folder: {gold_folder}"
    # should have at least one .parquet file
    parquet_files = list(gold_folder.glob("*.parquet"))
    assert parquet_files, f"No parquet files found in {gold_folder}"

    # ── 7) Validate HTML report ─────────────────────────────────────────────
    html_file = reports_dir / "desk_pnl_test_run.html"
    assert html_file.exists(), f"HTML report not found: {html_file}"
    content = html_file.read_text()
    # it should contain the heading and a <table> element
    assert "<table" in content and "Real P&L by Desk" in content
