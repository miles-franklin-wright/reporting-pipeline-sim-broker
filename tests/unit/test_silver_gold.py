# tests/unit/test_silver_gold.py

import re
import duckdb
import pytest
from pathlib import Path

# 1) Point to your dbt models folder
SQL_DIR = (
    Path(__file__)
    .resolve()
    .parents[2]      # up from tests/unit → tests → repo root
    / "src"
    / "sb_pipeline"
    / "models"
)

# 2) Jinja‑compiler stub (from your test_helpers)
from pipeline.utils.test_helpers import compile_model


@pytest.fixture
def con():
    c = duckdb.connect()
    yield c
    c.close()


def test_silver_trades_logic(con):
    # ——— 1) Seed an in‑memory raw_trades table ——————————————
    con.execute("""
        CREATE TABLE raw_trades (
            trade_id     VARCHAR,
            desk_id      VARCHAR,
            symbol       VARCHAR,
            quantity     DOUBLE,
            price        DOUBLE,
            real_pnl_usd DOUBLE,
            fee_usd      DOUBLE,
            trade_time   VARCHAR
        );
    """)
    con.execute(
        "INSERT INTO raw_trades VALUES "
        "('T1','D1','ABC',10.0,100.0,50.0,1.0,'2025-04-18T09:00:00');"
    )

    # ——— 2) Compile the dbt model into plain SELECT SQL ——————
    silver_sql = compile_model(SQL_DIR / "silver" / "silver_trades.sql")

    # ——— 3) Point it at our raw_trades, not a bronze table ————
    silver_sql = silver_sql.replace("bronze_trades", "raw_trades")

    # ——— 4) DuckDB needs an explicit CAST for ISO timestamps ————
    silver_sql = silver_sql.replace(
        "TO_TIMESTAMP(trade_time)",
        "CAST(trade_time AS TIMESTAMP)"
    )

    # ——— 5) Create the silver_trades table in DuckDB ——————
    con.execute(f"CREATE TABLE silver_trades AS {silver_sql}")

    # ——— 6) Now verify the single row made it through ——————
    rows = con.execute(
        "SELECT trade_id, desk_id, real_pnl_usd, fee_usd FROM silver_trades"
    ).fetchall()
    assert rows == [("T1", "D1", 50.0, 1.0)]


def test_fact_trade_and_pnl_daily_logic(con):
    # ——— 1) Seed silver_trades so fact_trade can read from it ————
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
          TIMESTAMP '2025-04-18 09:00:00' AS trade_ts
    """)

    # ——— 2) Compile fact_trade.sql ———————————————
    fact_trade_sql = compile_model(SQL_DIR / "gold" / "fact_trade.sql")

    # ——— 3) Create the fact_trade table ——————————————
    con.execute(f"CREATE TABLE fact_trade AS {fact_trade_sql}")

    # ——— 4) Verify its contents ————————————————
    # fetch raw rows
    raw = con.execute(
        "SELECT trade_id, desk_id, trade_date, real_pnl_usd, fee_usd FROM fact_trade"
    ).fetchall()

    # normalize types for comparison
    normalized = []
    for trade_id, desk_id, trade_date, real_pnl, fee in raw:
        # trade_date is datetime.date → string
        date_str = trade_date.isoformat() if hasattr(trade_date, "isoformat") else str(trade_date)
        # real_pnl and fee may be Decimal → float
        pnl_f = float(real_pnl)
        fee_f = float(fee)
        normalized.append((trade_id, desk_id, date_str, pnl_f, fee_f))

    assert normalized == [('T1', 'D1', '2025-04-18', 50.0, 1.0)]

    # ——— 5) Compile fact_pnl_daily.sql ——————————————
    pnl_sql = compile_model(SQL_DIR / "gold" / "fact_pnl_daily.sql")

    # ——— 6) Create the fact_pnl_daily table ——————————————
    con.execute(f"CREATE TABLE fact_pnl_daily AS {pnl_sql}")

    # ——— 7) Verify its aggregation ——————————————
    raw_pnl = con.execute(
        "SELECT desk_id, real_pnl_usd, fees_usd FROM fact_pnl_daily"
    ).fetchall()

    # normalize types: Decimal → float
    normalized_pnl = [
        (desk, float(rp), float(fee))
        for desk, rp, fee in raw_pnl
    ]

    assert normalized_pnl == [('D1', 50.0, 1.0)]
