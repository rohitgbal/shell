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
select a.scvpbb_vin_d_2 as vin,
a.scvpbb_icc_d_2 as iccid,
a.scvpbb_cvrt_ver_x_2 as cvrt_version,
a.scvpbb_esn_x_2 as esn,
a.scvpbb_enty_d_2 as entity_id,
a.scvpbb_enty_typ_x_2 as entity_type,
a.scvpbb_consent_stat_x_2 as consent_status,
a.scvpbb_consent_seq_id_2 as consent_sequence_id,
a.scvpbb_consent_strt_s_2 as consent_start_time,
a.scvpbb_pte_ver_x_2 as pte_version,
a.scvpbb_pte_s_2 as pte_timestamp,
a.scvpbb_ufm_ver_x_2 as ufm_version,
a.scvpbb_ufm_s_2 as ufm_timestamp,
a.scvpbb_scf_ver_x_2 as scf_version,
a.scvpbb_scf_s_2 as scf_timestamp,
b.save_region_code as partition_region,
b.save_dlr_cntry_cd as partition_country
 from (select scvpbb_vin_d_2,
   scvpbb_cvrt_ver_x_2,
   scvpbb_icc_d_2,
   scvpbb_esn_x_2,
   scvpbb_enty_d_2,
   scvpbb_enty_typ_x_2,
   scvpbb_consent_stat_x_2,
   scvpbb_consent_seq_id_2,
   scvpbb_consent_strt_s_2,
   scvpbb_pte_ver_x_2,
   scvpbb_pte_s_2,
   scvpbb_ufm_ver_x_2,
   scvpbb_ufm_s_2,
   scvpbb_scf_ver_x_2,
   scvpbb_scf_s_2,
   scvpbb_partition_cntry_x,
   scvpbb_partition_status_x
   from ${hivevar:mstr_table}
   lateral view explode(scvpbb_consent_seq_d_2) seqTable as scvpbb_consent_seq_id_2) a
 join ${hivevar:vin_meta_table} b
 on LPAD(a.scvpbb_vin_d_2,18,SUBSTR(a.scvpbb_vin_d_2,-1)) = b.vin
 where a.scvpbb_partition_cntry_x = 'UNKNOWN'
 and a.scvpbb_partition_status_x = 'OLD'
 and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(b.save_dlr_cntry_cd) <> '')
 union
 select
 c.scvpbb_vin_d_2,
 c.scvpbb_cvrt_ver_x_2,
 c.scvpbb_icc_d_2,
 c.scvpbb_esn_x_2,
 c.scvpbb_enty_d_2,
 c.scvpbb_enty_typ_x_2,
 c.scvpbb_consent_stat_x_2,
 scvpbb_consent_seq_id_2 as scvpbb_consent_seq_d_2,
 c.scvpbb_consent_strt_s_2,
 c.scvpbb_pte_ver_x_2,
 c.scvpbb_pte_s_2,
 c.scvpbb_ufm_ver_x_2,
 c.scvpbb_ufm_s_2,
 c.scvpbb_scf_ver_x_2,
 c.scvpbb_scf_s_2,
 c.scvpbb_partition_region_x,
 c.scvpbb_partition_cntry_x
 from ${hivevar:mstr_table} c lateral view explode(c.scvpbb_consent_seq_d_2) seqTable as scvpbb_consent_seq_id_2
 where c.scvpbb_vin_d_2 in(select distinct a.scvpbb_vin_d_2
 from ${hivevar:mstr_table} a
 join ${hivevar:vin_meta_table} b
 on LPAD(a.scvpbb_vin_d_2,18,SUBSTR(a.scvpbb_vin_d_2,-1)) = b.vin
 where a.scvpbb_partition_cntry_x = 'UNKNOWN'
 and a.scvpbb_partition_status_x = 'OLD'
 and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(b.save_dlr_cntry_cd) <> ''))
 and c.scvpbb_partition_status_x = 'OLD'
 and c.scvpbb_partition_cntry_x <> 'UNKNOWN';