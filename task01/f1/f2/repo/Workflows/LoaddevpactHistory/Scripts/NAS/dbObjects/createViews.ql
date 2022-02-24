create view IF NOT EXISTS NSCVVFYA_tcx_SCHEDULE_NA_USA_LI_VW
as
select
*
from  NSCVPFY_tcx_SCHED_HIST_PII_HTE
where scvpfy_partition_country_x = 'USA' ;

create view IF NOT EXISTS nscvvfya_tcx_schedule_eu_fra_li_vw as select * From NSCVPFY_tcx_SCHED_HIST_PII_HTE where scvpfy_partition_country_x='FRA';
create view IF NOT EXISTS nscvvfya_tcx_schedule_eu_deu_li_vw as select * From NSCVPFY_tcx_SCHED_HIST_PII_HTE where scvpfy_partition_country_x='DEU';
create view IF NOT EXISTS nscvvfya_tcx_schedule_eu_ita_li_vw as select * From NSCVPFY_tcx_SCHED_HIST_PII_HTE where scvpfy_partition_country_x='ITA';
create view IF NOT EXISTS nscvvfya_tcx_schedule_eu_esp_li_vw as select * From NSCVPFY_tcx_SCHED_HIST_PII_HTE where scvpfy_partition_country_x='ESP';
create view IF NOT EXISTS nscvvfya_tcx_schedule_eu_gbr_li_vw as select * From NSCVPFY_tcx_SCHED_HIST_PII_HTE where scvpfy_partition_country_x='GBR';
create view IF NOT EXISTS nscvvfya_tcx_schedule_na_can_li_vw as select * From NSCVPFY_tcx_SCHED_HIST_PII_HTE where scvpfy_partition_country_x='CAN';
