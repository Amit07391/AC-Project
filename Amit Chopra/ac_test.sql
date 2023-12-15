-- Databricks notebook source

select count(distinct ptid) as n_pat from ty18_rx_presc_subset
where rxdate between '2017-01-01' and '2021-12-31' and lcase(sub_CONDITION) in ('amiodarone','dofetilide','dronedarone','flecainide','propafenone','sotalol')

-- COMMAND ----------

select source_1st_rx_any_aad, count(*) as n_obs, count(distinct ptid) as n_pat, min(dt_1st_rx_any_aad) as dt_rx_start, max(dt_1st_rx_any_aad) as dt_rx_end
from ty18_any_aad_rx_af
where dt_1st_rx_any_aad between '2017-01-01' and '2021-12-31'
group by source_1st_rx_any_aad
;select source_1st_rx_any_aad, count(*) as n_obs, count(distinct ptid) as n_pat, min(dt_1st_rx_any_aad) as dt_rx_start, max(dt_1st_rx_any_aad) as dt_rx_end
from ty18_any_aad_rx_af
where dt_1st_rx_any_aad between '2017-01-01' and '2021-12-31'
and dt_1st_rx_any_aad>=dt_1st_any_af
group by source_1st_rx_any_aad
;

