use ${hivevar:db_name};


create table if not exists NSCVP99_H2T_NA_USA_SUMM_PII_HTE
(
   scvp99_vin_d_2 varchar(17),
   scvp99_cvrt_ver_r_2 string,
   scvp99_icc_d_2 string,
   scvp99_esn_x_2 string,
   scvp99_enty_d_2 int,
   scvp99_enty_typ_x_2 string,
   scvp99_consent_stat_x_2 string,
   scvp99_consent_seq_d_2 array<int>,
   scvp99_consent_strt_s_2 timestamp,
   scvp99_consent_end_s_2 timestamp,
   scvp99_pte_ver_r_2 string,
   scvp99_pte_s_2 timestamp,
   scvp99_ufm_ver_r_2 string,
   scvp99_ufm_s_2 timestamp,
   scvp99_rec_stat_x_2 string,
   scvp99_created_by_c varchar(10),
   scvp99_created_on_s timestamp,
   scvp99_partition_region_x string
)
partitioned by (scvp99_partition_cntry_x string, scvp99_partition_date_x string, scvp99_partition_status_x string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvp99_H2T_na_usa_summ_pii_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true');

create table if not exists NSCVP99_H2T_NA_NON_USA_SUMM_PII_HTE
(
   scvp99_vin_d_2 varchar(17),
   scvp99_cvrt_ver_r_2 string,
   scvp99_icc_d_2 string,
   scvp99_esn_x_2 string,
   scvp99_enty_d_2 int,
   scvp99_enty_typ_x_2 string,
   scvp99_consent_stat_x_2 string,
   scvp99_consent_seq_d_2 array<int>,
   scvp99_consent_strt_s_2 timestamp,
   scvp99_consent_end_s_2 timestamp,
   scvp99_pte_ver_r_2 string,
   scvp99_pte_s_2 timestamp,
   scvp99_ufm_ver_r_2 string,
   scvp99_ufm_s_2 timestamp,
   scvp99_rec_stat_x_2 string,
   scvp99_created_by_c varchar(10),
   scvp99_created_on_s timestamp,
   scvp99_partition_region_x string
)
partitioned by (scvp99_partition_cntry_x string, scvp99_partition_date_x string, scvp99_partition_status_x string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvp99_H2T_na_non_usa_summ_pii_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true');

create table if not exists NSCVP99_H2T_EU_GBR_FRA_SUMM_PII_HTE
(
   scvp99_vin_d_2 varchar(17),
   scvp99_cvrt_ver_r_2 string,
   scvp99_icc_d_2 string,
   scvp99_esn_x_2 string,
   scvp99_enty_d_2 int,
   scvp99_enty_typ_x_2 string,
   scvp99_consent_stat_x_2 string,
   scvp99_consent_seq_d_2 array<int>,
   scvp99_consent_strt_s_2 timestamp,
   scvp99_consent_end_s_2 timestamp,
   scvp99_pte_ver_r_2 string,
   scvp99_pte_s_2 timestamp,
   scvp99_ufm_ver_r_2 string,
   scvp99_ufm_s_2 timestamp,
   scvp99_rec_stat_x_2 string,
   scvp99_created_by_c varchar(10),
   scvp99_created_on_s timestamp,
   scvp99_partition_region_x string
)
partitioned by (scvp99_partition_cntry_x string, scvp99_partition_date_x string, scvp99_partition_status_x string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvp99_H2T_eu_gbr_fra_summ_pii_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true');

create table if not exists NSCVP99_H2T_EU_NON_GBR_FRA_SUMM_PII_HTE
(
   scvp99_vin_d_2 varchar(17),
   scvp99_cvrt_ver_r_2 string,
   scvp99_icc_d_2 string,
   scvp99_esn_x_2 string,
   scvp99_enty_d_2 int,
   scvp99_enty_typ_x_2 string,
   scvp99_consent_stat_x_2 string,
   scvp99_consent_seq_d_2 array<int>,
   scvp99_consent_strt_s_2 timestamp,
   scvp99_consent_end_s_2 timestamp,
   scvp99_pte_ver_r_2 string,
   scvp99_pte_s_2 timestamp,
   scvp99_ufm_ver_r_2 string,
   scvp99_ufm_s_2 timestamp,
   scvp99_rec_stat_x_2 string,
   scvp99_created_by_c varchar(10),
   scvp99_created_on_s timestamp,
   scvp99_partition_region_x string
)
partitioned by (scvp99_partition_cntry_x string, scvp99_partition_date_x string, scvp99_partition_status_x string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvp99_H2T_eu_non_gbr_fra_summ_pii_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true');


create table if not exists NSCVP99_H2T_AP_SUMM_PII_HTE
(
   scvp99_vin_d_2 varchar(17),
   scvp99_cvrt_ver_r_2 string,
   scvp99_icc_d_2 string,
   scvp99_esn_x_2 string,
   scvp99_enty_d_2 int,
   scvp99_enty_typ_x_2 string,
   scvp99_consent_stat_x_2 string,
   scvp99_consent_seq_d_2 array<int>,
   scvp99_consent_strt_s_2 timestamp,
   scvp99_consent_end_s_2 timestamp,
   scvp99_pte_ver_r_2 string,
   scvp99_pte_s_2 timestamp,
   scvp99_ufm_ver_r_2 string,
   scvp99_ufm_s_2 timestamp,
   scvp99_rec_stat_x_2 string,
   scvp99_created_by_c varchar(10),
   scvp99_created_on_s timestamp,
   scvp99_partition_region_x string
)
partitioned by (scvp99_partition_cntry_x string, scvp99_partition_date_x string, scvp99_partition_status_x string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvp99_H2T_ap_summ_pii_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true');

create table if not exists NSCVP99_H2T_RW_SUMM_PII_HTE
(
   scvp99_vin_d_2 varchar(17),
   scvp99_cvrt_ver_r_2 string,
   scvp99_icc_d_2 string,
   scvp99_esn_x_2 string,
   scvp99_enty_d_2 int,
   scvp99_enty_typ_x_2 string,
   scvp99_consent_stat_x_2 string,
   scvp99_consent_seq_d_2 array<int>,
   scvp99_consent_strt_s_2 timestamp,
   scvp99_consent_end_s_2 timestamp,
   scvp99_pte_ver_r_2 string,
   scvp99_pte_s_2 timestamp,
   scvp99_ufm_ver_r_2 string,
   scvp99_ufm_s_2 timestamp,
   scvp99_rec_stat_x_2 string,
   scvp99_created_by_c varchar(10),
   scvp99_created_on_s timestamp,
   scvp99_partition_region_x string
)
partitioned by (scvp99_partition_cntry_x string, scvp99_partition_date_x string, scvp99_partition_status_x string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvp99_H2T_rw_summ_pii_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true');

create external table if not exists NSCVS99_H2T_SUMM_ST_HTE  
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
   record_status string,
   created_by Varchar(10),
   created_on Timestamp,
   partition_region string,
   partition_cntry string,
   partition_date string,
   partition_status string
)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/nscvs99_H2T_summ_st_hte';

create external table if not exists NSCVS99_H2T_SUMM_TEMP_ST_HTE
(
   vin varchar(17),
   cvrt_version string,
   iccid string,
   esn string,
   entity_id int,
   entity_type string,
   consent_status string,
   consent_sequence_id int,
   consent_start_time timestamp,
   pte_version string,
   pte_timestamp timestamp,
   ufm_version string,
   ufm_timestamp timestamp,
   partition_region string,
   partition_country string
)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/nscvs99_H2T_summ_temp_st_hte';