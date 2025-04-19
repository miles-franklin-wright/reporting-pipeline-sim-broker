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
    Try to load the fact_pnl_daily parquet files from the gold layer.
    If none exist (or any error), return an empty list.
    """
    pattern = f"{lake_root}/gold/fact_pnl_daily/*.parquet"
    con = duckdb.connect()
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{pattern}')").df()
    except Exception:
        # swallow *any* read error and return empty
        df = pd.DataFrame(columns=["desk_id", "real_pnl_usd", "fees_usd"])
    finally:
        con.close()
    return df.to_dict(orient="records")

def print_summary_table(df: pd.DataFrame) -> None:
    console = Console()
    table = Table(show_header=True, header_style="bold blue")
    for col in df.columns:
        table.add_column(col)
    for _, row in df.iterrows():
        table.add_row(*(str(row[c]) for c in df.columns))
    console.print(table)
