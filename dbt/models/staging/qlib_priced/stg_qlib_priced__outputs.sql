--
-- FILE: `StockTrader/dbt/models/staging/qlib_priced/stg_qlib_priced__outputs.sql`
--

with source as (
	select
		market_date::DATE 		as market_date,
		occ::VARCHAR 			as occ,
		npv::DOUBLE 			as npv,
		Δ::DOUBLE 				as delta,
		Γ::DOUBLE 				as gamma,
		Θ::DOUBLE 				as theta,
		ν::DOUBLE 				as vega,
		ρ::DOUBLE 				as rho,
		σ_iv::DOUBLE 			as iv,
		npv_err::VARCHAR 		as npv_err,
		greek_err::VARCHAR 		as greek_err,
		σ_iv_err::VARCHAR 		as iv_err,
		model_name::VARCHAR 	as model_name,
		n_steps::INT 			as n_steps,
		compute_ms::DOUBLE 		as compute_ms,
		batch_id::VARCHAR 		as batch_id,
		shard::INT 				as shard,
		ingest_ts::TIMESTAMPTZ 	as ingest_ts
	from {{ source('qlib_priced', 'qlib_priced__bopm_dividends') }}
),

pricing_status as (
	select
		*,
		case
			when npv_err is not null then 'npv_fail'
			when greek_err is not null and iv_err is not null then 'greeks_iv_fail'
			when greek_err is not null then 'greeks_fail'
			when iv_err is not null then 'iv_fail'
			else 'ok'
		end as pricing_status
	from source
)

select * from pricing_status