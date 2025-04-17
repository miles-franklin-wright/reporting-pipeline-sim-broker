-- models/gold/fact_trade.sql
{{ config(materialized='table') }}
SELECT
  trade_id,
  desk_id,
  symbol,
  DATE(trade_ts)     AS trade_date,
  quantity,
  price,
  real_pnl_usd,
  fee_usd
FROM {{ ref('silver_trades') }};
