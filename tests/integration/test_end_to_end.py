import subprocess
import json
import duckdb
import pytest
from pathlib import Path

@pytest.fixture
def tmp_env(tmp_path):
    sim = tmp_path / "simulator_out"; sim.mkdir()
    lake = tmp_path / "lake_root";    lake.mkdir()
    rpt  = tmp_path / "reports";      rpt.mkdir()

    # minimal sample files for all Bronze inputs
    trade = {"trade_id":"T1","desk_id":"D1","symbol":"ABC",
             "quantity":5,"price":10,"real_pnl_usd":2,"fee_usd":0.1,
             "trade_time":"2025-04-18T09:00:00"}
    (sim/"trades.jsonl").write_text(json.dumps(trade)+"\n")

    order = {"order_id":"O1","desk_id":"D1","symbol":"ABC",
             "quantity":5,"price":10,"order_type":"MKT",
             "order_time":"2025-04-18T08:00:00"}
    (sim/"orders.jsonl").write_text(json.dumps(order)+"\n")

    (sim/"positions.csv").write_text(
        "position_id,desk_id,symbol,quantity,as_of_date\n"
        "P1,D1,ABC,5,2025-04-18\n"
    )
    (sim/"pnl.csv").write_text(
        "pnl_id,desk_id,real_pnl_usd,unreal_pnl_usd,as_of_date\n"
        "PN1,D1,2,1,2025-04-18\n"
    )

    # config for the pipeline
    cfg = tmp_path/"config.yml"
    cfg.write_text(f"""
input_dir: {sim}
lake_root: {lake}
reports_dir: {rpt}
run_id: test_0001
""".strip())
    return {"config": str(cfg), "lake": str(lake), "reports": str(rpt)}

def test_full_pipeline(tmp_env):
    cfg  = tmp_env["config"]
    lake = tmp_env["lake"]
    rpt  = tmp_env["reports"]

    # 1) run the full CLI: ingest → dbt → report
    proc = subprocess.run([
        "python", "-m", "pipeline.cli", "ingest",
        "--config", cfg,
        "--html-report", "--print-summary"
    ], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr

    con = duckdb.connect()

    # 2) Bronze checks
    # trades
    con.execute(f"CREATE VIEW vt AS SELECT * FROM read_json_auto('{lake}/bronze/trades/*.jsonl');")
    assert con.execute("SELECT COUNT(*) FROM vt").fetchone()[0] == 1
    # orders
    con.execute(f"CREATE VIEW vo AS SELECT * FROM read_json_auto('{lake}/bronze/orders/*.jsonl');")
    assert con.execute("SELECT COUNT(*) FROM vo").fetchone()[0] == 1
    # positions
    con.execute(f"CREATE VIEW vp AS SELECT * FROM read_csv_auto('{lake}/bronze/positions/*.csv');")
    assert con.execute("SELECT COUNT(*) FROM vp").fetchone()[0] == 1
    # pnl
    con.execute(f"CREATE VIEW vpn AS SELECT * FROM read_csv_auto('{lake}/bronze/pnl/*.csv');")
    assert con.execute("SELECT COUNT(*) FROM vpn").fetchone()[0] == 1

    # 3) Gold checks
    con.execute(f"CREATE VIEW ft AS SELECT trade_id, desk_id FROM read_parquet('{lake}/gold/fact_trade/*.parquet');")
    assert con.execute("SELECT * FROM ft").fetchone() == ("T1","D1")

    con.execute(f"CREATE VIEW fp AS SELECT desk_id, real_pnl_usd, fees_usd "
                f"FROM read_parquet('{lake}/gold/fact_pnl_daily/*.parquet');")
    assert con.execute("SELECT * FROM fp").fetchone() == ("D1",2.0,0.1)

    # 4) HTML report
    report_file = Path(rpt) / "desk_pnl_test_0001.html"
    assert report_file.exists()
    content = report_file.read_text()
    assert "D1" in content and "2.00" in content
