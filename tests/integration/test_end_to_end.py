# # tests/integration/test_end_to_end.py
# import pytest, shutil
# from click.testing import CliRunner
# from pathlib import Path
# from pipeline.cli import cli  # your click group/entrypoint
# import duckdb, json

# @pytest.fixture
# def tmp_env(tmp_path):
#     sim      = tmp_path/"sim";     sim.mkdir()
#     lake     = tmp_path/"lake";    lake.mkdir()
#     reports  = tmp_path/"reports"; reports.mkdir()

#     # write all four Bronze inputs into `sim`
#     (sim/"trades.jsonl").write_text(json.dumps({...})+"\n")
#     (sim/"orders.jsonl").write_text(json.dumps({...})+"\n")
#     (sim/"positions.csv").write_text("...csv header/data\n")
#     (sim/"pnl.csv").write_text("...csv header/data\n")

#     (tmp_path/"config.yml").write_text(f"""
# input_dir: {sim}
# lake_root: {lake}
# reports_dir: {reports}
# run_id: test_0001
# """)
#     return {"config": str(tmp_path/"config.yml"), "lake": lake, "reports": reports}

# def test_full_pipeline(tmp_env):
#     runner = CliRunner()
#     result = runner.invoke(cli, [
#         "ingest",
#         "--config", tmp_env["config"],
#         "--html-report",
#         "--print-summary"
#     ])
#     # 1) CLI must exit cleanly
#     assert result.exit_code == 0, result.output

#     # 2) Bronze directory now contains Delta files
#     assert (tmp_env["lake"]/"bronze"/"trades").exists()

#     # 3) Gold fact reads via DuckDB
#     con = duckdb.connect()
#     df = con.execute(
#         f"SELECT * FROM read_parquet('{tmp_env['lake']}/gold/fact_trade/*.parquet')"
#     ).fetchall()
#     assert len(df) == 1

#     # 4) HTML file exists & contains desk_id
#     report = tmp_env["reports"]/f"desk_pnl_test_0001.html"
#     assert report.exists()
#     assert "D1" in report.read_text()

#     # 5) cleanup (CliRunner’s tmp is ephemeral, but ensure any globals cleared)
#     shutil.rmtree(tmp_env["lake"])
#     shutil.rmtree(tmp_env["reports"])
