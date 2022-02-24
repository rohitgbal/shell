use ${hivevar:db_name};


create table if not exists NSCVPBB_H2T_SUMM_PJ1_PII_HTE  
(
   scvpbb_vin_d_2 varchar(17),
   scvpbb_cvrt_ver_x_2 string,
   scvpbb_icc_d_2 string,
   scvpbb_esn_x_2 string,
   scvpbb_enty_d_2 int,
   scvpbb_enty_typ_x_2 string,
   scvpbb_consent_stat_x_2 string,
   scvpbb_consent_seq_d_2 array<int>,
   scvpbb_consent_strt_s_2 timestamp,
   scvpbb_consent_end_s_2 timestamp,
   scvpbb_pte_ver_x_2 string,
   scvpbb_pte_s_2 timestamp,
   scvpbb_ufm_ver_x_2 string,
   scvpbb_ufm_s_2 timestamp,
   scvpbb_scf_ver_x_2 string,
   scvpbb_scf_s_2 timestamp,
   scvpbb_rec_stat_x_2 string,
   scvpbb_created_on_s timestamp,
   scvpbb_created_by_c varchar(10)
)
partitioned by (scvpbb_partition_region_x string, scvpbb_partition_cntry_x string, scvpbb_partition_date_x string, scvpbb_partition_status_x string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvpbb_H2T_summ_pj1_pii_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true','orc.compress.size'='8192');

create external table if not exists NSCVSBB_H2T_SUMM_PJ1_ST_HTE  
(
   vin varchar(17),
   cvrt_version string,
   icc_id string,
   esn string,
   entity_id int,
   entity_type string,
   consent_status string,
   consent_sequence_id array<int>,
   consent_start_time timestamp,
   consent_end_time timestamp,
   pte_version string,
   pte_timestamp timestamp,
   ufm_version string,
   ufm_timestamp timestamp,
   scf_version string,
   scf_timestamp timestamp,
   record_status string,
   created_on Timestamp,
   created_by Varchar(10),
   partition_region_x string,
   partition_cntry_x string,
   partition_date_x string,
   partition_status_x string
)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/nscvsbb_H2T_summ_pj1_st_hte';
