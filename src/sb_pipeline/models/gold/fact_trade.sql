-- src/sb_pipeline/models/gold/fact_trade.sql

{{ config(materialized='view') }}

SELECT
  trade_id,
  desk_id,
  symbol,
  -- cast the timestamp to a plain date for downstream grouping/tests
  CAST(trade_ts AS DATE) AS trade_date,
  quantity,
  price,
  real_pnl_usd,
  fee_usd
FROM {{ ref('silver_trades') }}
