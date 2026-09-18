--
-- FILE: `StockTrader/dbt/models/intermediate/acct_bal/int_acct_bal__aggs_to_daily.sql`
--

{{ config(materialized='view') }}

--
-- Wealth state variable = stg_tradier__acct_bal.value (Tradier total_equity)
--

with snaps as (
    select
        created_date as market_date,
        created_ts as ts, acct_id,
        acct_type,
        value,
        cash,
        option_buy_pwr,
        req_margin,
        req_option,
        market_value,
        option_value_short,
        option_value_long,
        pnl_open,
        pnl_close,
        n_orders_pending
    from {{ ref('stg_tradier__acct_bal') }}
    where isodow(created_date) between 1 and 5
    and value > 0
    qualify row_number() over (partition by acct_id, created_ts order by value desc) = 1
),


--
-- Intraday Log Step t

intraday_steps as (
    select
        *,
        ln(value / nullif(lag(value) over w, 0)) as step_log_return
        from snaps
        window w as (partition by acct_id, market_date order by ts)
),

daily as (
    select
        acct_id,
        any_value(acct_type) as acct_type,
        market_date,
        count(*) as n_snaps,
        min(ts) as ts_0,
        max(ts) as ts_1,

        -- Intraday Wealth Stats
        arg_min(value, ts) as w_open,
        arg_max(value, ts) as w_close,
        min(value) as w_low,
        max(value) as w_high,
        sum(step_log_return * step_log_return) as w_rv,

        -- EOD Balance Sheet State
        arg_max(cash, ts) as cash,
        arg_max(option_buy_pwr, ts) as option_buy_pwr,
        arg_max(req_margin, ts) as req_margin,
        arg_max(req_option, ts) as req_option,
        arg_max(market_value, ts) as market_value,
        arg_max(option_value_short, ts) as option_value_short,
        arg_max(option_value_long, ts) as option_value_long,
        arg_max(pnl_open, ts) as pnl_open,
        arg_max(pnl_close, ts) as pnl_close,
        arg_max(n_orders_pending, ts) as n_orders_pending
    from intraday_steps
    group by acct_id, market_date
),
daily_wealth as (
    select
        acct_id,
        acct_type,
        market_date,
        n_snaps,
        ts_0,
        ts_1,
        w_open,
        w_high,
        w_low,
        w_close,
        (w_high-w_low) / nullif(w_open,0) as w_range,
        sqrt(w_rv) as w_rv_sqrt,
        ln(w_high / nullif(w_low,0)) / (2 * sqrt(ln(2.0))) as w_vol_park,
        cash,
        option_buy_pwr,
        req_margin,
        req_option,
        market_value,
        option_value_short,
        option_value_long,
        pnl_open,
        pnl_close,
        n_orders_pending
    from daily
)
select * from daily_wealth;
