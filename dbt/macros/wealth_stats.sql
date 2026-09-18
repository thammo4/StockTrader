--
-- FILE: `StockTrader/dbt/macros/wealth_stats.sql`
--

{% macro growth_power_sums(w, sfx) %}
	count(log_return) over {{ w }}					as n_obs{{ sfx }},
	sum(log_return) over {{ w }}					as log_growth_cum{{ sfx }},
	sum(n_sessions_gap) over {{ w }}				as n_sessions_spanned{{ sfx }},
	avg(log_return) over {{ w }}					as g_m1{{ sfx }},
	stddev_samp(log_return) over {{ w }}			as g_sd{{ sfx }},
	avg(return_simple) over {{ w }}					as r_m1{{ sfx }},
	avg(pow(return_simple, 2)) over {{ w }}			as r_e2{{ sfx }},
	avg(pow(return_simple, 3)) over {{ w }}			as r_e3{{ sfx }},
	avg(pow(return_simple, 4)) over {{ w }}			as r_e4{{ sfx }},
	avg(req_frac_wealth) over {{ w }}				as req_frac_mean{{ sfx }}
{% endmacro %}


{#
	Central moments from raw power sums, POPULATION form (divide by n):
	  m2 = E[X^2] - mu^2
	  m3 = E[X^3] - 3*mu*E[X^2] + 2*mu^3
	  m4 = E[X^4] - 4*mu*E[X^3] + 6*mu^2*E[X^2] - 3*mu^4
	Population is used for the Kelly terms because the expansion is a statement
	about the distribution. Sample (n-1) stddev is used for the SE/t-stat because
	that is the inference convention. The two differ by n/(n-1) ~ 4% at n=26,
	which is noise against a +/-37% standard error, but the split is explicit so
	any number this model prints is reproducible by hand.
#}
{% macro growth_derived(sfx) %}
	n_obs{{ sfx }},

	{% set m2 %}(r_e2{{ sfx }} - pow(r_m1{{ sfx }}, 2)){% endset %}
	{% set m3 %}(r_e3{{ sfx }} - 3 * r_m1{{ sfx }} * r_e2{{ sfx }} + 2 * pow(r_m1{{ sfx }}, 3)){% endset %}
	{% set m4 %}(r_e4{{ sfx }} - 4 * r_m1{{ sfx }} * r_e3{{ sfx }} + 6 * pow(r_m1{{ sfx }}, 2) * r_e2{{ sfx }} - 3 * pow(r_m1{{ sfx }}, 4)){% endset %}

	10000.0 * g_m1{{ sfx }}														as g_mean_bp{{ sfx }},
	10000.0 * g_sd{{ sfx }}														as g_sd_bp{{ sfx }},
	g_sd{{ sfx }} * sqrt({{ var('trading_days_per_annum') }})					as g_vol_ann{{ sfx }},

	-- naive: annualize the per-observation mean
	g_m1{{ sfx }} * {{ var('trading_days_per_annum') }}							as g_ann{{ sfx }},
	exp(g_m1{{ sfx }} * {{ var('trading_days_per_annum') }}) - 1				as cagr_implied{{ sfx }},

	-- calendar-honest: spread cumulative log growth over sessions actually spanned
	case when n_sessions_spanned{{ sfx }} > 0
		then log_growth_cum{{ sfx }} * {{ var('trading_days_per_annum') }}
			/ n_sessions_spanned{{ sfx }} end									as g_ann_sessions{{ sfx }},

	-- estimator precision
	g_sd{{ sfx }} / sqrt(nullif(n_obs{{ sfx }}, 0))								as g_mean_se{{ sfx }},
	g_sd{{ sfx }} * sqrt({{ var('trading_days_per_annum') }})
		* sqrt({{ var('trading_days_per_annum') }} / nullif(n_obs{{ sfx }}, 0))	as g_ann_mean_se{{ sfx }},
	g_m1{{ sfx }} / nullif(g_sd{{ sfx }} / sqrt(nullif(n_obs{{ sfx }}, 0)), 0)	as g_mean_t{{ sfx }},

	-- shape
	{{ m2 }}																	as r_m2{{ sfx }},
	{{ m3 }}																	as r_m3{{ sfx }},
	{{ m4 }}																	as r_m4{{ sfx }},
	{{ m3 }} / nullif(pow({{ m2 }}, 1.5), 0)									as r_skew{{ sfx }},
	{{ m4 }} / nullif(pow({{ m2 }}, 2), 0) - 3.0								as r_excess_kurt{{ sfx }},

	-- lambda* under a normal approximation
	(r_m1{{ sfx }} - {{ var('rf_log_daily') }}) / nullif({{ m2 }}, 0)			as kelly_gaussian{{ sfx }},

	-- lambda* with the third-order term:
	--   g(l) = l*mu - l^2*m2/2 + l^3*m3/3
	--   l*   = (m2 - sqrt(m2^2 - 4*mu*m3)) / (2*m3)
	-- Strictly below mu/m2 whenever m3 < 0. Valid only while |l*r| << 1, which
	-- is why kelly_empirical (model 4) is the number to act on.
	case
		when {{ m3 }} is null or abs({{ m3 }}) < 1e-14
			then (r_m1{{ sfx }} - {{ var('rf_log_daily') }}) / nullif({{ m2 }}, 0)
		when pow({{ m2 }}, 2) - 4 * (r_m1{{ sfx }} - {{ var('rf_log_daily') }}) * {{ m3 }} < 0
			then null
		else ({{ m2 }} - sqrt(pow({{ m2 }}, 2)
			- 4 * (r_m1{{ sfx }} - {{ var('rf_log_daily') }}) * {{ m3 }})) / (2 * {{ m3 }})
	end																			as kelly_third_order{{ sfx }},

	req_frac_mean{{ sfx }}
{% endmacro %}
