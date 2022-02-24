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
select scvpbb_vin_d_2 as vin,scvpbb_cvrt_ver_x_2 as cvrt_version,scvpbb_icc_d_2 as iccid,scvpbb_esn_x_2 as esn,scvpbb_enty_d_2 as entity_id,scvpbb_enty_typ_x_2 as entity_type,
scvpbb_consent_stat_x_2 as consent_status,scvpbb_consent_seq_id_2 as consent_sequence_id,scvpbb_consent_strt_s_2 as consent_start_time,scvpbb_pte_ver_x_2 as pte_version,
scvpbb_pte_s_2 as pte_timestamp, scvpbb_ufm_ver_x_2 as ufm_version,scvpbb_ufm_s_2 as ufm_timestamp,scvpbb_scf_ver_x_2 as scf_version,scvpbb_scf_s_2 as scf_timestamp,
scvpbb_partition_region_x as partition_region,scvpbb_partition_cntry_x as partition_country
from ${hivevar:mstr_table}
lateral view explode(scvpbb_consent_seq_d_2) seqTable as scvpbb_consent_seq_id_2
where scvpbb_partition_status_x = 'NEW';