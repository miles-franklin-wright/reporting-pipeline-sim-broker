# src/pipeline/cli.py
import sys
import subprocess
import yaml
import click
from pathlib import Path

from pipeline.utils.spark_helper import get_spark
from pipeline.ingest.bronze_writer import ingest_jsonl, ingest_csv
from pipeline.reporting.html_report import generate_html_report
from pipeline.utils.duck_helpers import fetch_pnl_daily, print_summary_table

@click.group()
def cli():
    """Reporting Service CLI."""
    pass

@cli.command()
@click.option("--config", "-c", required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Path to ingestion config YAML")
@click.option("--html-report/--no-html-report", default=False,
              help="Generate HTML report to reports_dir")
@click.option("--print-summary/--no-print-summary", default=False,
              help="Print a console summary after run")
def ingest(config, html_report, print_summary):
    """
    Run full pipeline: ingest → dbt build → report → summary.
    """
    # --- 1) Load config
    cfg = yaml.safe_load(Path(config).read_text())
    input_dir   = Path(cfg["input_dir"])
    lake_root   = Path(cfg["lake_root"])
    reports_dir = Path(cfg["reports_dir"])
    run_id      = cfg["run_id"]

    # --- 2) Start Spark with user config
    spark_conf = {}
    spark_conf.update(cfg.get("spark", {}))
    spark_conf.update(cfg.get("hadoop", {}))
    spark = get_spark(spark_conf)

    # --- 3) Ingest Bronze layer with run_id subfolders
    counts = {}
    counts["trades"] = ingest_jsonl(
        spark,
        str(input_dir/"trades.jsonl"),
        str(lake_root/"bronze"/"trades"/run_id),
        skip_write=False
    )
    counts["orders"] = ingest_jsonl(
        spark,
        str(input_dir/"orders.jsonl"),
        str(lake_root/"bronze"/"orders"/run_id),
        skip_write=False
    )
    counts["positions"] = ingest_csv(
        spark,
        str(input_dir/"positions.csv"),
        str(lake_root/"bronze"/"positions"/run_id),
        skip_write=False
    )
    counts["pnl"] = ingest_csv(
        spark,
        str(input_dir/"pnl.csv"),
        str(lake_root/"bronze"/"pnl"/run_id),
        skip_write=False
    )
    click.echo(f"✔ Ingested Bronze: {counts}")

    # --- 4) Prepare and run dbt build
    project_dir = Path(__file__).parent.parent / "sb_pipeline"
    profiles_dir = project_dir

    # Sanity checks for dbt files
    if not (project_dir / "dbt_project.yml").exists():
        click.secho(f"Error: couldn't find dbt_project.yml in {project_dir}", fg="red", err=True)
        sys.exit(1)
    if not (profiles_dir / "profiles.yml").exists():
        click.secho(f"Error: couldn't find profiles.yml in {profiles_dir}", fg="red", err=True)
        sys.exit(1)

    dbt_cmd = [
        sys.executable, "-m", "dbt", "build",
        "--project-dir", str(project_dir),
        "--profiles-dir", str(profiles_dir),
    ]
    res = subprocess.run(dbt_cmd, capture_output=True, text=True)
    if res.returncode != 0:
        click.secho(res.stderr, fg="red")
        sys.exit(res.returncode)
    click.secho("✔ dbt build succeeded", fg="green")

    # --- 5) HTML report?
    if html_report:
        html_path = generate_html_report(
            lake_root=str(lake_root),
            run_id=run_id,
            output_dir=str(reports_dir),
        )
        click.secho(f"✔ HTML report written to {html_path}", fg="green")

    # --- 6) Console summary?
    if print_summary:
        rows = fetch_pnl_daily(str(lake_root))
        import pandas as pd
        df = pd.DataFrame(rows)
        print_summary_table(df)

if __name__ == "__main__":
    cli()
