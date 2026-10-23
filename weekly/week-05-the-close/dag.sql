/* dag.yml
dag: the_close
schedule: nightly
catchup: true
tasks:
  payments_history:
    materialized: incremental
    incremental_strategy: append
    depends_on: [raw_payments]
  fx_rates_history:
    materialized: incremental
    incremental_strategy: append
    depends_on: [raw_fx_rates]
  merchants_history:
    materialized: incremental
    incremental_strategy: append
    depends_on: [raw_merchants]
  fct_payments:
    materialized: table
    depends_on: [payments_history, fx_rates_history, merchants_history]
    tests:
      - unique: payment_id
*/

-- each file is 1 SELECT, built into a table of the file's name; dag.yml says what it runs after

-- task: payments_history
SELECT * FROM raw_payments
{% if is_incremental() %}
WHERE _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
{% endif %}
;

-- task: fx_rates_history
SELECT * FROM raw_fx_rates
{% if is_incremental() %}
WHERE _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
{% endif %}
;

-- task: merchants_history
SELECT * FROM raw_merchants
{% if is_incremental() %}
WHERE _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
{% endif %}
;

-- task: fct_payments
SELECT 'payments_history' AS payment_id,
       (SELECT group_concat(name, ',') FROM pragma_table_info('payments_history')) AS status,
       NULL AS amount_usd, NULL AS settled_usd, NULL AS settled_in_period_usd,
       NULL AS captured_at, NULL AS order_ref, NULL AS merchant_country
UNION ALL
SELECT 'fx_rates_history',
       (SELECT group_concat(name, ',') FROM pragma_table_info('fx_rates_history')),
       NULL, NULL, NULL, NULL, NULL, NULL
UNION ALL
SELECT 'merchants_history',
       (SELECT group_concat(name, ',') FROM pragma_table_info('merchants_history')),
       NULL, NULL, NULL, NULL, NULL, NULL
