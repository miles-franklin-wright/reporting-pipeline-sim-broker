# src/pipeline/utils/duck_helpers.py
import duckdb
import pandas as pd
from rich.table import Table
from rich.console import Console
from typing import List, Dict

def load_duck_table(path_pattern: str) -> pd.DataFrame:
    """
    Read all Parquet files matching the glob into a Pandas DataFrame.
    """
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT * 
        FROM read_parquet('{path_pattern}')
    """).df()
    con.close()
    return df

def fetch_pnl_daily(lake_root: str) -> List[Dict]:
    """
    Load the fact_pnl_daily parquet from gold layer and return
    a list of dicts [ {desk_id, real_pnl_usd, fees_usd}, ... ].
    """
    df = load_duck_table(f"{lake_root}/gold/fact_pnl_daily/*.parquet")
    # Convert floats back to native Python types if desired
    return df.to_dict(orient="records")

def print_summary_table(df: pd.DataFrame) -> None:
    """
    Pretty‑print a Pandas DataFrame to the console using Rich.
    """
    console = Console()
    table = Table(show_header=True, header_style="bold blue")
    # Add columns
    for col in df.columns:
        table.add_column(col)
    # Add rows
    for _, row in df.iterrows():
        table.add_row(*(str(row[c]) for c in df.columns))
    console.print(table)
