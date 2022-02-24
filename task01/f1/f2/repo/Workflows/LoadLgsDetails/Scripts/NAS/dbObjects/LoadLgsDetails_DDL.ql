use ${hivevar:db_name};


create external table if not exists NCVDSX8_WIL_SUMM_ST_HTE
(
   source_sha_key string,
   wil_sha_key string,
   vin varchar(17),
   event_time timestamp,
   number int,
   longitude double,
   latitude double,
   source varchar(10),
   partition_region string,
   auth_stat_c string,
   raw_payload_metadata_cvrt_ver_r string,
   wil varchar(6),
   wil_description varchar(200),
   transform_status varchar(20),
   lifecycmde_d_actl_r string,
   process_status string,
   process_status_date_time_utc timestamp,
   cvdcx8_created_on_s timestamp,
   cvdcx8_created_by_c VARCHAR(10),
   partition_country string,
   partition_date string
)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsx8_wil_summ_st_hte';


create  table if not exists NCVDCX8_WIL_SUMM_ACID_SEC_HTE
(
   cvdcx8_src_sha_k string,
   cvdcx8_wil_sha_k string,
   cvdcx8_vin_d_2 varchar(17),
   cvdcx8_event_s timestamp,
   cvdcx8_nump_r_2 int,
   cvdcx8_long_r_3 double,
   cvdcx8_lat_r_3 double,
    cvdcx8_src_c varchar(10),
    cvdcx8_rgn_x string,
    cvdcx8_auth_stat_c string,
    cvdcx8_raw_payload_metadata_cvrt_ver_r string,
   cvdcx8_wil_c varchar(6),
   cvdcx8_wil_desc_x varchar(200),
   cvdcx8_tfm_stat_x varchar(20),
   cvdcx8_lifecycmde_d_actl_r string,
   cvdcx8_process_stat_c string,
   cvdcx8_process_stat_utc_s timestamp,
   cvdcx8_created_on_s TIMESTAMP,
   cvdcx8_created_by_c VARCHAR(10)
)
partitioned by (cvdcx8_partition_cntry_x string, cvdcx8_partition_date_x string)
clustered by (cvdcx8_vin_d_2) into 10 buckets
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/Common/LgsDetails/ncvdcx8_wil_summ_acid_sec_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true','orc.compress.size'='8192');




-- TSTROUTE (PRS)
create table if not exists NCVDCX8_WIL_SUMM_PRS_ACID_SEC_HTE
(
   cvdcx8_src_sha_k string,
   cvdcx8_wil_sha_k string,
   cvdcx8_vin_d_2 varchar(17),
   cvdcx8_event_s timestamp,
   cvdcx8_nump_r_2 int,
   cvdcx8_long_r_3 double,
   cvdcx8_lat_r_3 double,
    cvdcx8_src_c varchar(10),
    cvdcx8_rgn_x string,
    cvdcx8_auth_stat_c string,
    cvdcx8_raw_payload_metadata_cvrt_ver_r string,
   cvdcx8_wil_c varchar(6),
   cvdcx8_wil_desc_x varchar(200),
   cvdcx8_tfm_stat_x varchar(20),
   cvdcx8_lifecycmde_d_actl_r string,
   cvdcx8_process_stat_c string,
   cvdcx8_process_stat_utc_s timestamp,
   cvdcx8_created_on_s TIMESTAMP,
   cvdcx8_created_by_c VARCHAR(10)
)
partitioned by (cvdcx8_partition_cntry_x string, cvdcx8_partition_date_x string)
clustered by (cvdcx8_vin_d_2) into 10 buckets
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/Common/LgsDetails/ncvdcx8_wil_summ_prs_acid_sec_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true','orc.compress.size'='8192');

create external table if not exists NCVDSX8_WIL_SUMM_PRS_ST_HTE
(
   source_sha_key string,
   wil_sha_key string,
   vin varchar(17),
   event_time timestamp,
   number int,
   longitude double,
   latitude double,
   source varchar(10),
   partition_region string,
   auth_stat_c string,
   raw_payload_metadata_cvrt_ver_r string,
   wil varchar(6),
   wil_description varchar(200),
   transform_status varchar(20),
   lifecycmde_d_actl_r string,
   process_status string,
   process_status_date_time_utc timestamp,
   cvdcx8_created_on_s timestamp,
   cvdcx8_created_by_c VARCHAR(10),
   partition_country string,
   partition_date string
)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsx8_wil_summ_prs_st_hte';

---------------- Wil Summary PSA Tables ----------------

create external table if not exists NCVDSX8_WIL_SUMM_PSA_ST_HTE
(
   source_sha_key string,
   wil_sha_key string,
   vin varchar(17),
   event_time timestamp,
   number int,
   longitude double,
   latitude double,
   source varchar(10),
   partition_region string,
   auth_stat_c string,
   raw_payload_metadata_cvrt_ver_r string,
   wil varchar(6),
   wil_description varchar(200),
   transform_status varchar(20),
   lifecycmde_d_actl_r string,
   process_status string,
   process_status_date_time_utc timestamp,
   cvdcx8_created_on_s timestamp,
   cvdcx8_created_by_c VARCHAR(10),
   partition_country string,
   partition_date string
)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsx8_wil_summ_psa_st_hte';

create table if not exists NCVDCX8_WIL_SUMM_ACID_PSA_SEC_HTE
(
   cvdcx8_src_sha_k string,
   cvdcx8_wil_sha_k string,
   cvdcx8_vin_d_2 varchar(17),
   cvdcx8_event_s timestamp,
   cvdcx8_nump_r_2 int,
   cvdcx8_long_r_3 double,
   cvdcx8_lat_r_3 double,
    cvdcx8_src_c varchar(10),
    cvdcx8_rgn_x string,
    cvdcx8_auth_stat_c string,
    cvdcx8_raw_payload_metadata_cvrt_ver_r string,
   cvdcx8_wil_c varchar(6),
   cvdcx8_wil_desc_x varchar(200),
   cvdcx8_tfm_stat_x varchar(20),
   cvdcx8_lifecycmde_d_actl_r string,
   cvdcx8_process_stat_c string,
   cvdcx8_process_stat_utc_s timestamp,
   cvdcx8_created_on_s TIMESTAMP,
   cvdcx8_created_by_c VARCHAR(10)
)
partitioned by (cvdcx8_partition_cntry_x string, cvdcx8_partition_date_x string)
clustered by (cvdcx8_vin_d_2) into 10 buckets
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/Common/LgsDetails/ncvdcx8_wil_summ_acid_psa_sec_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true','orc.compress.size'='8192');

---------------- Wil Summary PSU Tables ----------------

create external table if not exists NCVDSX8_WIL_SUMM_PSU_ST_HTE
(
   source_sha_key string,
   wil_sha_key string,
   vin varchar(17),
   event_time timestamp,
   number int,
   longitude double,
   latitude double,
   source varchar(10),
   partition_region string,
   auth_stat_c string,
   raw_payload_metadata_cvrt_ver_r string,
   wil varchar(6),
   wil_description varchar(200),
   transform_status varchar(20),
   lifecycmde_d_actl_r string,
   process_status string,
   process_status_date_time_utc timestamp,
   cvdcx8_created_on_s timestamp,
   cvdcx8_created_by_c VARCHAR(10),
   partition_country string,
   partition_date string
)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsx8_wil_summ_psu_st_hte';

create  table if not exists NCVDCX8_WIL_SUMM_ACID_PSU_SEC_HTE
(
   cvdcx8_src_sha_k string,
   cvdcx8_wil_sha_k string,
   cvdcx8_vin_d_2 varchar(17),
   cvdcx8_event_s timestamp,
   cvdcx8_nump_r_2 int,
   cvdcx8_long_r_3 double,
   cvdcx8_lat_r_3 double,
    cvdcx8_src_c varchar(10),
    cvdcx8_rgn_x string,
    cvdcx8_auth_stat_c string,
    cvdcx8_raw_payload_metadata_cvrt_ver_r string,
   cvdcx8_wil_c varchar(6),
   cvdcx8_wil_desc_x varchar(200),
   cvdcx8_tfm_stat_x varchar(20),
   cvdcx8_lifecycmde_d_actl_r string,
   cvdcx8_process_stat_c string,
   cvdcx8_process_stat_utc_s timestamp,
   cvdcx8_created_on_s TIMESTAMP,
   cvdcx8_created_by_c VARCHAR(10)
)
partitioned by (cvdcx8_partition_cntry_x string, cvdcx8_partition_date_x string)
clustered by (cvdcx8_vin_d_2) into 10 buckets
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/Common/LgsDetails/ncvdcx8_wil_summ_acid_psu_sec_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true','orc.compress.size'='8192');

alter table Nprg2_WORKFLOW_reccalc_ADM_HTE add if not exists partition (prg2_file_typ_c='LgsDetails4G-PostSalesAuth');
alter table Nprg2_WORKFLOW_reccalc_ADM_HTE add if not exists partition (prg2_file_typ_c='LgsDetails4G-PostSalesUnAuth');
