use ${hivevar:db_name};


create external table if not exists NCVDSX8_WIL_SUMM_PJ1_ST_HTE
(
   source_sha_key string,
   wil_sha_key string,
   vin varchar(17),
   event_time timestamp,
   number int,
   longitude double,
   latitude double,
   source varchar(10),
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
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsx8_wil_summ_pj1_st_hte';



create external table if not exists NCVDCX8_WIL_SUMM_PJ1_SEC_HTE
(
   cvdcx8_src_sha_k string,
   cvdcx8_wil_sha_k string,
   cvdcx8_vin_d_2 varchar(17),
   cvdcx8_event_s timestamp,
   cvdcx8_nump_r_2 int,
   cvdcx8_long_r_3 double,
   cvdcx8_lat_r_3 double,
    cvdcx8_src_c varchar(10),
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
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/Common/LgsDetails/ncvdcx8_wil_summ_pj1_sec_hte'
tblproperties ('orc compress size'='8192');

