-- models/silver/silver_trades.sql
{{ config(materialized='view') }}
SELECT
  trade_id,
  desk_id,
  symbol,
  CAST(quantity AS DOUBLE)     AS quantity,
  CAST(price AS DOUBLE)        AS price,
  CAST(real_pnl_usd AS DOUBLE) AS real_pnl_usd,
  CAST(fee_usd AS DOUBLE)      AS fee_usd,
  TO_TIMESTAMP(trade_time)     AS trade_ts
FROM read_delta('{{ var("lake_root") }}/bronze/trades')
WHERE trade_id IS NOT NULL;
