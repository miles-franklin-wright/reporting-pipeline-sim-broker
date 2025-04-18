-- src/sb_pipeline/models/silver/silver_trades.sql

{{ config(
    materialized='table'
) }}

WITH raw AS (
  SELECT 
    trade_id,
    desk_id,
    symbol,
    quantity,
    price,
    real_pnl_usd,
    fee_usd,
    trade_time
  FROM {{ ref('bronze_trades') }}
  WHERE trade_id IS NOT NULL
)

SELECT
  trade_id,
  desk_id,
  symbol,
  quantity,
  price,
  real_pnl_usd,
  fee_usd,
  -- parse the ISO timestamp string into a proper timestamp
  TO_TIMESTAMP(trade_time) AS trade_ts
FROM raw
;
