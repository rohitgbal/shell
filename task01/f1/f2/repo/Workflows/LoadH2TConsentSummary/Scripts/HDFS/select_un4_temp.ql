use ${hivevar:db_name};

set hive.execution.engine=tez;
set hive.exec.orc.split.strategy=BI;
set hive.orc.cache.use.soft.references=true;
set hive.tez.java.opts=-Xmx24576m;
set hive.tez.container.size=32768;
set tez.am.resource.memory.mb=16384;
set hive.tez.java.opts=-server -Xmx16384m -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps;
set hive.exec.max.dynamic.partitions=10000;
set hive.msck.path.validation=ignore;



insert into table ${hivevar:temp_stg_table}
select scvp99_vin_d_2 as vin,scvp99_cvrt_ver_r_2 as cvrt_version,scvp99_icc_d_2 as iccid,scvp99_esn_x_2 as esn,scvp99_enty_d_2 as entity_id,
scvp99_enty_typ_x_2 as entity_type,scvp99_consent_stat_x_2 as consent_status,seqId as consent_sequence_id,scvp99_consent_strt_s_2 as consent_start_time,
scvp99_pte_ver_r_2 as pte_version, scvp99_pte_s_2 as pte_timestamp, scvp99_ufm_ver_r_2 as ufm_version,scvp99_ufm_s_2 as ufm_timestamp,
scvp99_partition_region_x as partition_region,scvp99_partition_cntry_x as partition_country
from ${hivevar:mstr_na_usa_table} lateral view explode(scvp99_consent_seq_d_2) seqTable as seqId
where scvp99_partition_status_x = 'NEW' and scvp99_rec_stat_x_2 = 'NEW'and scvp99_consent_strt_s_2 != scvp99_consent_end_s_2;

insert into table ${hivevar:temp_stg_table}
select scvp99_vin_d_2 as vin,scvp99_cvrt_ver_r_2 as cvrt_version,scvp99_icc_d_2 as iccid,scvp99_esn_x_2 as esn,scvp99_enty_d_2 as entity_id,
scvp99_enty_typ_x_2 as entity_type,scvp99_consent_stat_x_2 as consent_status,seqId as consent_sequence_id,scvp99_consent_strt_s_2 as consent_start_time,
scvp99_pte_ver_r_2 as pte_version, scvp99_pte_s_2 as pte_timestamp, scvp99_ufm_ver_r_2 as ufm_version,scvp99_ufm_s_2 as ufm_timestamp,
scvp99_partition_region_x as partition_region,scvp99_partition_cntry_x as partition_country
from ${hivevar:mstr_na_non_usa_table} lateral view explode(scvp99_consent_seq_d_2) seqTable as seqId
where scvp99_partition_status_x = 'NEW' and scvp99_rec_stat_x_2 = 'NEW'and scvp99_consent_strt_s_2 != scvp99_consent_end_s_2;

insert into table ${hivevar:temp_stg_table}
select scvp99_vin_d_2 as vin,scvp99_cvrt_ver_r_2 as cvrt_version,scvp99_icc_d_2 as iccid,scvp99_esn_x_2 as esn,scvp99_enty_d_2 as entity_id,
scvp99_enty_typ_x_2 as entity_type,scvp99_consent_stat_x_2 as consent_status,seqId as consent_sequence_id,scvp99_consent_strt_s_2 as consent_start_time,
scvp99_pte_ver_r_2 as pte_version, scvp99_pte_s_2 as pte_timestamp, scvp99_ufm_ver_r_2 as ufm_version,scvp99_ufm_s_2 as ufm_timestamp,
scvp99_partition_region_x as partition_region,scvp99_partition_cntry_x as partition_country
from ${hivevar:mstr_eu_gbr_fra_table} lateral view explode(scvp99_consent_seq_d_2) seqTable as seqId
where scvp99_partition_status_x = 'NEW' and scvp99_rec_stat_x_2 = 'NEW'and scvp99_consent_strt_s_2 != scvp99_consent_end_s_2;

insert into table ${hivevar:temp_stg_table}
select scvp99_vin_d_2 as vin,scvp99_cvrt_ver_r_2 as cvrt_version,scvp99_icc_d_2 as iccid,scvp99_esn_x_2 as esn,scvp99_enty_d_2 as entity_id,
scvp99_enty_typ_x_2 as entity_type,scvp99_consent_stat_x_2 as consent_status,seqId as consent_sequence_id,scvp99_consent_strt_s_2 as consent_start_time,
scvp99_pte_ver_r_2 as pte_version, scvp99_pte_s_2 as pte_timestamp, scvp99_ufm_ver_r_2 as ufm_version,scvp99_ufm_s_2 as ufm_timestamp,
scvp99_partition_region_x as partition_region,scvp99_partition_cntry_x as partition_country
from ${hivevar:mstr_eu_non_gbr_fra_table} lateral view explode(scvp99_consent_seq_d_2) seqTable as seqId
where scvp99_partition_status_x = 'NEW' and scvp99_rec_stat_x_2 = 'NEW'and scvp99_consent_strt_s_2 != scvp99_consent_end_s_2;

insert into table ${hivevar:temp_stg_table}
select scvp99_vin_d_2 as vin,scvp99_cvrt_ver_r_2 as cvrt_version,scvp99_icc_d_2 as iccid,scvp99_esn_x_2 as esn,scvp99_enty_d_2 as entity_id,
scvp99_enty_typ_x_2 as entity_type,scvp99_consent_stat_x_2 as consent_status,seqId as consent_sequence_id,scvp99_consent_strt_s_2 as consent_start_time,
scvp99_pte_ver_r_2 as pte_version, scvp99_pte_s_2 as pte_timestamp, scvp99_ufm_ver_r_2 as ufm_version,scvp99_ufm_s_2 as ufm_timestamp,
scvp99_partition_region_x as partition_region,scvp99_partition_cntry_x as partition_country
from ${hivevar:mstr_ap_table} lateral view explode(scvp99_consent_seq_d_2) seqTable as seqId
where scvp99_partition_status_x = 'NEW' and scvp99_rec_stat_x_2 = 'NEW'and scvp99_consent_strt_s_2 != scvp99_consent_end_s_2;

insert into table ${hivevar:temp_stg_table}
select scvp99_vin_d_2 as vin,scvp99_cvrt_ver_r_2 as cvrt_version,scvp99_icc_d_2 as iccid,scvp99_esn_x_2 as esn,scvp99_enty_d_2 as entity_id,
scvp99_enty_typ_x_2 as entity_type,scvp99_consent_stat_x_2 as consent_status,seqId as consent_sequence_id,scvp99_consent_strt_s_2 as consent_start_time,
scvp99_pte_ver_r_2 as pte_version, scvp99_pte_s_2 as pte_timestamp, scvp99_ufm_ver_r_2 as ufm_version,scvp99_ufm_s_2 as ufm_timestamp,
scvp99_partition_region_x as partition_region,scvp99_partition_cntry_x as partition_country
from ${hivevar:mstr_rw_table} lateral view explode(scvp99_consent_seq_d_2) seqTable as seqId
where scvp99_partition_status_x = 'NEW' and scvp99_rec_stat_x_2 = 'NEW'and scvp99_consent_strt_s_2 != scvp99_consent_end_s_2;