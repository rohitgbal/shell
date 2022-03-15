use ${hivevar:db_name};


create external table if not exists NCVDSVT_AXT_SUMMARY_ST_HTE  
(
   cvdsvt_sha_k string,
   cvdsvt_sha_k_AXT string,
   cvdsvt_vin_d_3 varchar(17),
   cvdsvt_msg_metadata_msg_n string,
   cvdsvt_event_s_3 timestamp,
   cvdsvt_nump_r_2 decimal(11,1),
   cvdsvt_lon_r_3 double,
   cvdsvt_lat_r_3 double,
   cvdsvt_ecu_d_2 string,
   cvdsvt_AXT_d_2 string,
   cvdsvt_AXT_info_byte_x_2 string,
   cvdsvt_AXT_status_byte_x_2 string,
   cvdsvt_test_fald_r_2 int,
   cvdsvt_test_fald_this_oper_cyc_r_2 int,
   cvdsvt_pend_AXT_r_2 int,
   cvdsvt_confmd_AXT_r_2 int,
   cvdsvt_test_not_cmpltd_since_lst_clr_r_2 int,
   cvdsvt_test_fald_since_lst_clr_r_2 int,
   cvdsvt_test_not_cmpltd_this_oper_cyc_r_2 int,
   cvdsvt_wrng_ind_reqstd_r_2 int,
   cvdsvt_src_c varchar(10),
   cvdsvt_region_x string,
   cvdsvt_created_by_c varchar(10),
   cvdsvt_created_on_s TIMESTAMP,
   cvdsvt_raw_payload_metadata_cvrt_ver_x string,
   cvdsvt_auth_stat_c string,
   cvdsvt_lifecycmde_d_actl_r string
)
partitioned by (cvdsvt_partition_cntry_x string,cvdsvt_partition_date_x string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsvt_AXT_summary_st_hte'
tblproperties ('orc.compress.size'='8192');

create table if not exists NCVDCVT_AXT_SUMMARY_SEC_ACID_HTE  
(
   cvdcvt_sha_k string,
   cvdcvt_sha_k_AXT string,
   cvdcvt_vin_d_3 varchar(17),
   cvdcvt_event_s_3 timestamp,
   cvdcvt_nump_r_2 decimal(11,1),
   cvdcvt_lon_r_3 double,
   cvdcvt_lat_r_3 double,
   cvdcvt_ecu_d_2 string,
   cvdcvt_AXT_d_2 string,
   cvdcvt_AXT_info_byte_x_2 string,
   cvdcvt_AXT_status_byte_x_2 string,
   cvdcvt_test_fald_r_2 int,
   cvdcvt_test_fald_this_oper_cyc_r_2 int,
   cvdcvt_pend_AXT_r_2 int,
   cvdcvt_confmd_AXT_r_2 int,
   cvdcvt_test_not_cmpltd_since_lst_clr_r_2 int,
   cvdcvt_test_fald_since_lst_clr_r_2 int,
   cvdcvt_test_not_cmpltd_this_oper_cyc_r_2 int,
   cvdcvt_wrng_ind_reqstd_r_2 int,
   cvdcvt_src_c varchar(10),
   cvdcvt_region_x string,
   cvdcvt_created_by_c VARCHAR(10),
   cvdcvt_created_on_s TIMESTAMP,
   cvdcvt_raw_payload_metadata_cvrt_ver_x string,
   cvdcvt_auth_stat_c string,
   cvdcvt_lifecycmde_d_actl_r string
)
partitioned by (cvdcvt_partition_cntry_x string,cvdcvt_partition_date_x string)
clustered by (cvdcvt_vin_d_3) into 10 buckets
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/AXTSummary/ncvdcvt_AXT_summary_sec_acid_hte'
tblproperties ('orc.compress.size'='8192','transactional'='true');



--TSTROUTE (PRS)
create external table if not exists ncvdsvt_AXT_summary_prs_st_hte  
(
   cvdsvt_sha_k string,
   cvdsvt_sha_k_AXT string,
   cvdsvt_vin_d_3 varchar(17),
   cvdsvt_msg_metadata_msg_n string,
   cvdsvt_event_s_3 timestamp,
   cvdsvt_nump_r_2 decimal(11,1),
   cvdsvt_lon_r_3 double,
   cvdsvt_lat_r_3 double,
   cvdsvt_ecu_d_2 string,
   cvdsvt_AXT_d_2 string,
   cvdsvt_AXT_info_byte_x_2 string,
   cvdsvt_AXT_status_byte_x_2 string,
   cvdsvt_test_fald_r_2 int,
   cvdsvt_test_fald_this_oper_cyc_r_2 int,
   cvdsvt_pend_AXT_r_2 int,
   cvdsvt_confmd_AXT_r_2 int,
   cvdsvt_test_not_cmpltd_since_lst_clr_r_2 int,
   cvdsvt_test_fald_since_lst_clr_r_2 int,
   cvdsvt_test_not_cmpltd_this_oper_cyc_r_2 int,
   cvdsvt_wrng_ind_reqstd_r_2 int,
   cvdsvt_src_c varchar(10),
   cvdsvt_region_x string,
   cvdsvt_created_by_c varchar(10),
   cvdsvt_created_on_s TIMESTAMP,
   cvdsvt_raw_payload_metadata_cvrt_ver_x string,
   cvdsvt_auth_stat_c string,
   cvdsvt_lifecycmde_d_actl_r string
)
partitioned by (cvdsvt_partition_cntry_x string,cvdsvt_partition_date_x string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsvt_AXT_summary_prs_st_hte'
tblproperties ('orc.compress.size'='8192');

create table if not exists NCVDCVT_AXT_SUMMARY_PRS_SEC_ACID_HTE
(
   cvdcvt_sha_k string,
   cvdcvt_sha_k_AXT string,
   cvdcvt_vin_d_3 varchar(17),
   cvdcvt_event_s_3 timestamp,
   cvdcvt_nump_r_2 decimal(11,1),
   cvdcvt_lon_r_3 double,
   cvdcvt_lat_r_3 double,
   cvdcvt_ecu_d_2 string,
   cvdcvt_AXT_d_2 string,
   cvdcvt_AXT_info_byte_x_2 string,
   cvdcvt_AXT_status_byte_x_2 string,
   cvdcvt_test_fald_r_2 int,
   cvdcvt_test_fald_this_oper_cyc_r_2 int,
   cvdcvt_pend_AXT_r_2 int,
   cvdcvt_confmd_AXT_r_2 int,
   cvdcvt_test_not_cmpltd_since_lst_clr_r_2 int,
   cvdcvt_test_fald_since_lst_clr_r_2 int,
   cvdcvt_test_not_cmpltd_this_oper_cyc_r_2 int,
   cvdcvt_wrng_ind_reqstd_r_2 int,
   cvdcvt_src_c varchar(10),
   cvdcvt_region_x string,
   cvdcvt_created_by_c VARCHAR(10),
   cvdcvt_created_on_s TIMESTAMP,
   cvdcvt_raw_payload_metadata_cvrt_ver_x string,
   cvdcvt_auth_stat_c string,
   cvdcvt_lifecycmde_d_actl_r string
)
partitioned by (cvdcvt_partition_cntry_x string,cvdcvt_partition_date_x string)
clustered by (cvdcvt_vin_d_3) into 10 buckets
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/AXTSummary/ncvdcvt_AXT_summary_prs_sec_acid_hte'
tblproperties ('orc.compress.size'='8192','transactional'='true');

-------------------------------AXT SUMMARY PSA TABLES------------------------------------------

create external table if not exists ncvdsvt_AXT_summary_psa_st_hte  
(
   cvdsvt_sha_k string,
   cvdsvt_sha_k_AXT string,
   cvdsvt_vin_d_3 varchar(17),
   cvdsvt_msg_metadata_msg_n string,
   cvdsvt_event_s_3 timestamp,
   cvdsvt_nump_r_2 decimal(11,1),
   cvdsvt_lon_r_3 double,
   cvdsvt_lat_r_3 double,
   cvdsvt_ecu_d_2 string,
   cvdsvt_AXT_d_2 string,
   cvdsvt_AXT_info_byte_x_2 string,
   cvdsvt_AXT_status_byte_x_2 string,
   cvdsvt_test_fald_r_2 int,
   cvdsvt_test_fald_this_oper_cyc_r_2 int,
   cvdsvt_pend_AXT_r_2 int,
   cvdsvt_confmd_AXT_r_2 int,
   cvdsvt_test_not_cmpltd_since_lst_clr_r_2 int,
   cvdsvt_test_fald_since_lst_clr_r_2 int,
   cvdsvt_test_not_cmpltd_this_oper_cyc_r_2 int,
   cvdsvt_wrng_ind_reqstd_r_2 int,
   cvdsvt_src_c varchar(10),
   cvdsvt_region_x string,
   cvdsvt_created_by_c varchar(10),
   cvdsvt_created_on_s TIMESTAMP,
   cvdsvt_raw_payload_metadata_cvrt_ver_x string,
   cvdsvt_auth_stat_c string,
   cvdsvt_lifecycmde_d_actl_r string
)
partitioned by (cvdsvt_partition_cntry_x string,cvdsvt_partition_date_x string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsvt_AXT_summary_psa_st_hte'
tblproperties ('orc.compress.size'='8192');

create table if not exists NCVDCVT_AXT_SUMMARY_ACID_PSA_SEC_HTE  
(
   cvdcvt_sha_k string,
   cvdcvt_sha_k_AXT string,
   cvdcvt_vin_d_3 varchar(17),
   cvdcvt_event_s_3 timestamp,
   cvdcvt_nump_r_2 decimal(11,1),
   cvdcvt_lon_r_3 double,
   cvdcvt_lat_r_3 double,
   cvdcvt_ecu_d_2 string,
   cvdcvt_AXT_d_2 string,
   cvdcvt_AXT_info_byte_x_2 string,
   cvdcvt_AXT_status_byte_x_2 string,
   cvdcvt_test_fald_r_2 int,
   cvdcvt_test_fald_this_oper_cyc_r_2 int,
   cvdcvt_pend_AXT_r_2 int,
   cvdcvt_confmd_AXT_r_2 int,
   cvdcvt_test_not_cmpltd_since_lst_clr_r_2 int,
   cvdcvt_test_fald_since_lst_clr_r_2 int,
   cvdcvt_test_not_cmpltd_this_oper_cyc_r_2 int,
   cvdcvt_wrng_ind_reqstd_r_2 int,
   cvdcvt_src_c varchar(10),
   cvdcvt_region_x string,
   cvdcvt_created_by_c VARCHAR(10),
   cvdcvt_created_on_s TIMESTAMP,
   cvdcvt_raw_payload_metadata_cvrt_ver_x string,
   cvdcvt_auth_stat_c string,
   cvdcvt_lifecycmde_d_actl_r string
)
partitioned by (cvdcvt_partition_cntry_x string,cvdcvt_partition_date_x string)
clustered by (cvdcvt_vin_d_3) into 10 buckets
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/AXTSummary/ncvdcvt_AXT_summary_acid_psa_sec_hte'
tblproperties ('orc.compress.size'='8192','transactional'='true');

-------------------------------AXT SUMMARY PSU TABLES------------------------------------------

create external table if not exists ncvdsvt_AXT_summary_psu_st_hte  
(
   cvdsvt_sha_k string,
   cvdsvt_sha_k_AXT string,
   cvdsvt_vin_d_3 varchar(17),
   cvdsvt_msg_metadata_msg_n string,
   cvdsvt_event_s_3 timestamp,
   cvdsvt_nump_r_2 decimal(11,1),
   cvdsvt_lon_r_3 double,
   cvdsvt_lat_r_3 double,
   cvdsvt_ecu_d_2 string,
   cvdsvt_AXT_d_2 string,
   cvdsvt_AXT_info_byte_x_2 string,
   cvdsvt_AXT_status_byte_x_2 string,
   cvdsvt_test_fald_r_2 int,
   cvdsvt_test_fald_this_oper_cyc_r_2 int,
   cvdsvt_pend_AXT_r_2 int,
   cvdsvt_confmd_AXT_r_2 int,
   cvdsvt_test_not_cmpltd_since_lst_clr_r_2 int,
   cvdsvt_test_fald_since_lst_clr_r_2 int,
   cvdsvt_test_not_cmpltd_this_oper_cyc_r_2 int,
   cvdsvt_wrng_ind_reqstd_r_2 int,
   cvdsvt_src_c varchar(10),
   cvdsvt_region_x string,
   cvdsvt_created_by_c varchar(10),
   cvdsvt_created_on_s TIMESTAMP,
   cvdsvt_raw_payload_metadata_cvrt_ver_x string,
   cvdsvt_auth_stat_c string,
   cvdsvt_lifecycmde_d_actl_r string
)
partitioned by (cvdsvt_partition_cntry_x string,cvdsvt_partition_date_x string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsvt_AXT_summary_psu_st_hte'
tblproperties ('orc.compress.size'='8192');

create table if not exists NCVDCVT_AXT_SUMMARY_ACID_PSU_SEC_HTE  
(
   cvdcvt_sha_k string,
   cvdcvt_sha_k_AXT string,
   cvdcvt_vin_d_3 varchar(17),
   cvdcvt_event_s_3 timestamp,
   cvdcvt_nump_r_2 decimal(11,1),
   cvdcvt_lon_r_3 double,
   cvdcvt_lat_r_3 double,
   cvdcvt_ecu_d_2 string,
   cvdcvt_AXT_d_2 string,
   cvdcvt_AXT_info_byte_x_2 string,
   cvdcvt_AXT_status_byte_x_2 string,
   cvdcvt_test_fald_r_2 int,
   cvdcvt_test_fald_this_oper_cyc_r_2 int,
   cvdcvt_pend_AXT_r_2 int,
   cvdcvt_confmd_AXT_r_2 int,
   cvdcvt_test_not_cmpltd_since_lst_clr_r_2 int,
   cvdcvt_test_fald_since_lst_clr_r_2 int,
   cvdcvt_test_not_cmpltd_this_oper_cyc_r_2 int,
   cvdcvt_wrng_ind_reqstd_r_2 int,
   cvdcvt_src_c varchar(10),
   cvdcvt_region_x string,
   cvdcvt_created_by_c VARCHAR(10),
   cvdcvt_created_on_s TIMESTAMP,
   cvdcvt_raw_payload_metadata_cvrt_ver_x string,
   cvdcvt_auth_stat_c string,
   cvdcvt_lifecycmde_d_actl_r string
)
partitioned by (cvdcvt_partition_cntry_x string,cvdcvt_partition_date_x string)
clustered by (cvdcvt_vin_d_3) into 10 buckets
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/AXTSummary/ncvdcvt_AXT_summary_acid_psu_sec_hte'
tblproperties ('orc.compress.size'='8192','transactional'='true');

--Tables Added for AXT Message Monitoring--
Create external table if not exists NCVDSVT_AXT_SUMMARY_Monitor_ST_HTE
(
cvdsvt_msg_metadata_msg_n string)
stored as orc

location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsvt_AXT_summary_monitor_st_hte'
tblproperties ('orc.compress.size'='8192');


Create external table if not exists NCVDCVT_AXT_SUMMARY_MSG_MNTR_HTE
(
cvdcvt_msg_metadata_msg_n string,
cvdcvt_created_by_c string,
cvdcvt_created_on_s timestamp)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/AXTSummary/ncvdcvt_AXT_summary_msg_mntr_hte'
tblproperties ('orc.compress.size'='8192','transactional'='true');


Create external table if not exists ncvdsvt_AXT_summary_monitor_psu_st_hte
(
cvdsvt_msg_metadata_msg_n string)
stored as orc

location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsvt_AXT_summary_monitor_psu_st_hte'
tblproperties ('orc.compress.size'='8192');


Create external table if not exists ncvdcvt_AXT_summary_msg_psu_mntr_hte
(
cvdcvt_msg_metadata_msg_n string,
cvdcvt_created_by_c string,
cvdcvt_created_on_s timestamp)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/AXTSummary/ncvdcvt_AXT_summary_msg_psu_mntr_hte'
tblproperties ('orc.compress.size'='8192');

Create external table if not exists ncvdsvt_AXT_summary_monitor_psa_st_hte
(
cvdsvt_msg_metadata_msg_n string)
stored as orc

location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsvt_AXT_summary_monitor_psa_st_hte'
tblproperties ('orc.compress.size'='8192');


Create external table if not exists ncvdcvt_AXT_summary_msg_psa_mntr_hte
(
cvdcvt_msg_metadata_msg_n string,
cvdcvt_created_by_c string,
cvdcvt_created_on_s timestamp)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/AXTSummary/ncvdcvt_AXT_summary_msg_psa_mntr_hte'
tblproperties ('orc.compress.size'='8192');


Create external table if not exists ncvdsvt_AXT_summary_monitor_prs_st_hte
(
cvdsvt_msg_metadata_msg_n string)
stored as orc

location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsvt_AXT_summary_monitor_prs_st_hte'
tblproperties ('orc.compress.size'='8192');


Create external table if not exists ncvdcvt_AXT_summary_msg_prs_mntr_hte
(
cvdcvt_msg_metadata_msg_n string,
cvdcvt_created_by_c string,
cvdcvt_created_on_s timestamp)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/AXTSummary/ncvdcvt_AXT_summary_msg_prs_mntr_hte'
tblproperties ('orc.compress.size'='8192');


alter table Nprg2_WORKFLOW_reccalc_ADM_HTE add if not exists partition (prg2_file_typ_c='dataprogRepository-AXTSummary-PostSalesAuth');
alter table Nprg2_WORKFLOW_reccalc_ADM_HTE add if not exists partition (prg2_file_typ_c='dataprogRepository-AXTSummary-PostSalesUnAuth');
