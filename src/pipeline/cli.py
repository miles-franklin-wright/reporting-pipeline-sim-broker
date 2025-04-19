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
@click.option("--dbt-dir", "-d", required=False,
              type=click.Path(exists=True, file_okay=False),
              help="Path to sb_pipeline dbt project directory")
@click.option("--html-report/--no-html-report", default=False,
              help="Generate HTML report to reports_dir")
@click.option("--print-summary/--no-print-summary", default=False,
              help="Print a console summary after run")
def ingest(config, dbt_dir, html_report, print_summary):
    """
    Run full pipeline: ingest → dbt build → report → summary.
    """
    # 1) Load user config
    cfg = yaml.safe_load(Path(config).read_text())
    input_dir = Path(cfg["input_dir"])
    lake_root = Path(cfg["lake_root"])
    reports_dir = Path(cfg["reports_dir"])
    run_id = str(cfg["run_id"])

    # 2) Start Spark with overrides
    spark_conf = {}
    spark_conf.update(cfg.get("spark") or {})
    spark_conf.update(cfg.get("hadoop") or {})
    spark = get_spark(spark_conf)

    # 3) Ingest raw artefacts
    counts = {}
    for entity, reader, ext in [
        ("trades", ingest_jsonl, "jsonl"),
        ("orders", ingest_jsonl, "jsonl"),
        ("positions", ingest_csv, "csv"),
        ("pnl", ingest_csv, "csv"),
    ]:
        src = input_dir / f"{entity}.{ext}"
        dest = lake_root / "bronze" / entity / run_id
        counts[entity] = reader(
            spark, str(src), str(dest), skip_write=False
        )
    click.echo(f"Ingested Bronze: {counts}")

    # 4) Locate and run dbt build
    project_dir = Path(dbt_dir) if dbt_dir else None
    if not project_dir:
        here = Path(__file__).resolve()
        for cand in [
            here.parent.parent / "sb_pipeline",      # src/sb_pipeline
            here.parent.parent.parent / "sb_pipeline" # repo_root/sb_pipeline
        ]:
            if cand.exists():
                project_dir = cand
                break
    if project_dir and (project_dir / "dbt_project.yml").exists():
        res = subprocess.run(
            ["dbt", "build", "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)],
            capture_output=True, text=True
        )
        if res.returncode != 0:
            click.secho(res.stderr, fg="red", err=True)
            sys.exit(res.returncode)
        click.secho("dbt build succeeded", fg="green")
    else:
        click.secho(
            "Warning: couldn’t find sb_pipeline project; skipping dbt build (use --dbt-dir)",
            fg="yellow", err=True
        )

    # 5) Generate HTML report
    if html_report:
        html_path = generate_html_report(
            lake_root=str(lake_root), run_id=run_id, output_dir=str(reports_dir)
        )
        click.secho(f"HTML report written to {html_path}", fg="green")

    # 6) Print summary table
    if print_summary:
        rows = fetch_pnl_daily(str(lake_root))
        import pandas as pd
        df = pd.DataFrame(rows)
        print_summary_table(df)

if __name__ == "__main__":
    cli()
