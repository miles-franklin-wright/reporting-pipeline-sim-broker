-- models/gold/fact_pnl_daily.sql
{{ config(materialized='table') }}
SELECT
  desk_id,
  SUM(real_pnl_usd) AS real_pnl_usd,
  SUM(fee_usd)      AS fees_usd
FROM {{ ref('silver_trades') }}
GROUP BY desk_id;
