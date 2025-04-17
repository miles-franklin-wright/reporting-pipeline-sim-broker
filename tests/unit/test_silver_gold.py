import duckdb
import pytest
import re
from pathlib import Path

# Point at your dbt SQL models under src/
BASE = Path(__file__).parents[2]
SQL_DIR = BASE / "src" / "sb_pipeline" / "models"

def load_sql(path: Path) -> str:
    return path.read_text()

@pytest.fixture
def con():
    return duckdb.connect()

def test_silver_trades_logic(con):
    # 1) create raw_trades table
    con.execute("""
        CREATE TABLE raw_trades (
            trade_id      VARCHAR,
            desk_id       VARCHAR,
            symbol        VARCHAR,
            quantity      DOUBLE,
            price         DOUBLE,
            real_pnl_usd  DOUBLE,
            fee_usd       DOUBLE,
            trade_time    VARCHAR
        );
    """)
    con.execute("""
        INSERT INTO raw_trades VALUES
        ('T1','D1','ABC',10.0,100.0,50.0,1.0,'2025-04-18T09:00:00');
    """)
    # 2) run silver_trades.sql, rewriting read_delta(...) → raw_trades
    sql = load_sql(SQL_DIR / "silver" / "silver_trades.sql")
    sql = re.sub(r"FROM\s+read_delta\(.+\)", "FROM raw_trades", sql)
    con.execute(sql)
    # 3) assert
    row = con.execute(
        "SELECT trade_id, desk_id, symbol, quantity, price, real_pnl_usd, fee_usd FROM silver_trades"
    ).fetchone()
    assert row == ("T1", "D1", "ABC", 10.0, 100.0, 50.0, 1.0)

def test_fact_trade_and_pnl_daily_logic(con):
    # seed silver_trades
    con.execute("""
        CREATE TABLE silver_trades AS
        SELECT
          'T1' AS trade_id,
          'D1' AS desk_id,
          'ABC' AS symbol,
          10.0 AS quantity,
          100.0 AS price,
          50.0 AS real_pnl_usd,
          1.0  AS fee_usd,
          TIMESTAMP '2025-04-18 09:00:00' AS trade_ts;
    """)
    # fact_trade
    sql_trade = load_sql(SQL_DIR / "gold" / "fact_trade.sql")
    sql_trade = re.sub(r"\{\{\s*ref\(['\"]silver_trades['\"]\)\s*\}\}", "silver_trades", sql_trade)
    con.execute(sql_trade)
    trade = con.execute(
        "SELECT trade_id, desk_id, symbol, quantity, price, real_pnl_usd, fee_usd FROM fact_trade"
    ).fetchone()
    assert trade == ("T1", "D1", "ABC", 10.0, 100.0, 50.0, 1.0)

    # fact_pnl_daily
    sql_pnl = load_sql(SQL_DIR / "gold" / "fact_pnl_daily.sql")
    sql_pnl = re.sub(r"\{\{\s*ref\(['\"]silver_trades['\"]\)\s*\}\}", "silver_trades", sql_pnl)
    con.execute(sql_pnl)
    agg = con.execute(
        "SELECT desk_id, real_pnl_usd, fees_usd FROM fact_pnl_daily"
    ).fetchone()
    assert agg == ("D1", 50.0, 1.0)
