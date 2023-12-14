-- Databricks notebook source

create or replace table ac_ehr_soliqua_optn_2_rx_wrx as
select distinct a.*, b.rx_type,b.gnrc_nm, b.brnd_nm, c.dt_rx_index 
from ac_ehr_WRx_202305  a join ty00_ses_rx_anti_dm_loopup b on a.ndc=b.ndc
inner join ac_ehr_pat_rx_bas_bol_sol_index c on a.ptid=c.ptid
inner join ac_ehr_soliqua_attrition_optn_2_final d on a.ptid=d.ptid
where a.RXDATE>='2017-01-01' and a.RXDATE<='2021-12-31'
order by a.ptid, a.RXDATE;

select distinct * from ac_ehr_soliqua_optn_2_rx_wrx
order by ptid, RXDATE;
