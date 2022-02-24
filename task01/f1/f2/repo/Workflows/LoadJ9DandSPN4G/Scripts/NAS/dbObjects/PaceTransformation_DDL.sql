use ${hivevar:db_name};

 create external table if not exists NCVDSUL_PACE_TCU_STRAT_PART_NO_ST_HTE
 ( 
    cvdcul_vin_d_3 varchar(17), 
    cvdcul_tcu_strat_part_no_c_3 varchar(20), 
    cvdcul_cvrt_ver_c_3 varchar(20), 
    cvdcul_first_assoc_s_3 timestamp, 
    cvdcul_last_assoc_s_3 timestamp, 
    cvdcul_event_s_3 timestamp, 
    cvdcul_latest_msg_n_3 varchar(255),
    cvdcul_partition_country_x  string,
    cvdcul_created_on_s timestamp, 
    cvdcul_created_by_c varchar(10)
 ) 
 ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
 STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
 OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
 LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsul_pace_tcu_strat_part_no_st_hte' 
 TBLPROPERTIES ('orc.compress.size'='8192'); 

create external table if not exists NCVDSUM_PACE_CMD_NAME_RSPNS_ST_HTE  
(
   vin varchar(17),
   esn varchar(15),
   correlation_id int,
   app_correlation_id varchar(150),
   command_name varchar(60),
   command_timestamp timestamp,
   command_status varchar(150),
   command_response_timestamp timestamp,
   duration int,
   wakeup_sms_flag varchar(5),
   cvrt_version varchar(20),
   authorization_status varchar(30),
   anonymization_flag varchar(30),
   cvrt_error_code string,
   cvrt_error_detail string,
   command_activity_id varchar(150),
   command_response_activity_id varchar(150),
   source varchar(15),
   tcu_strategy_part_number varchar(20),
   command_pk varchar(64),
   suspect_flag boolean
)
partitioned by (partition_region string, partition_country string, partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsum_pace_cmd_name_rspns_st_hte'
TBLPROPERTIES ('orc.compress.size'='8192');


 create external table if not exists NCVDCUL_PACE_TCU_STRAT_PART_NO_SEC_HTE
 (
    cvdcul_vin_d_3 varchar(17),
    cvdcul_tcu_strat_part_no_c_3 varchar(20),
    cvdcul_cvrt_ver_c_3 varchar(20),
    cvdcul_first_assoc_s_3 timestamp,
    cvdcul_last_assoc_s_3 timestamp,
    cvdcul_event_s_3 timestamp,
    cvdcul_latest_msg_n_3 varchar(255),
    cvdcul_created_on_s timestamp,
    cvdcul_created_by_c varchar(10)
 )
 partitioned by (cvdcul_partition_country_x  string)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
 STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
 LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/PaceTransformation/ncvdcul_pace_tcu_strat_part_no_sec_hte'
 tblproperties ('orc.compress.size'='8192');


create external table if not exists NCVDSUL_PACE_TCUECGSYNC_STRAT_PART_NO_ST_HTE
(
    cvdcul_vin_d_3 varchar(17),
    cvdcul_source varchar(255),
    cvdcul_tcu_strat_part_no_c_3 varchar(20),
    cvdcul_cvrt_ver_c_3 varchar(20),
    cvdcul_first_assoc_s_3 timestamp,
    cvdcul_last_assoc_s_3 timestamp,
    cvdcul_event_s_3 timestamp,
    cvdcul_latest_msg_n_3 varchar(255),
    cvdcul_module_type_d_3 varchar(255),
    cvdcul_partition_country_x  string,
    cvdcul_created_on_s timestamp,
    cvdcul_created_by_c varchar(10)
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/ncvdsul_pace_tcuecgsync_strat_part_no_st_hte'
    TBLPROPERTIES ('orc.compress.size'='8192');

 create external table if not exists NCVDCUL_PACE_STRAT_PART_NO_SEC_HTE
 (
    cvdcul_vin_d_3 varchar(17),
    cvdcul_source varchar(255),
    cvdcul_strat_part_no_c_3 varchar(20),
    cvdcul_cvrt_ver_c_3 varchar(20),
    cvdcul_first_assoc_s_3 timestamp,
    cvdcul_last_assoc_s_3 timestamp,
    cvdcul_event_s_3 timestamp,
    cvdcul_latest_msg_n_3 varchar(255),
    cvdcul_module_type_d_3 varchar(255),
    cvdcul_created_on_s timestamp,
    cvdcul_created_by_c varchar(10)
 )
 partitioned by (cvdcul_partition_country_x  string)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
 STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
 LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/ncvdcul_pace_strat_part_no_sec_hte'
 tblproperties ('orc.compress.size'='8192');

create external table if not exists NCVDCUM_PACE_CMD_NAME_RSPNS_SEC_HTE  
(
   cvdcum_vin_d_3 varchar(17),
   cvdcum_esn_d_3 varchar(15),
   cvdcum_crltn_d_3 int,
   cvdcum_app_crltn_d_3 varchar(150),
   cvdcum_cmd_n_3 varchar(60),
   cvdcum_cmd_s_3 timestamp,
   cvdcum_cmd_stat_x_3 varchar(150),
   cvdcum_cmd_rspns_s_3 timestamp,
   cvdcum_durn_r_3 int,
   cvdcum_wkup_sms_f_3 varchar(5),
   cvdcum_cvrt_ver_c_3 varchar(20),
   cvdcum_auth_stat_c_3 varchar(30),
   cvdcum_anon_f_3 varchar(30),
   cvdcum_cvrt_err_c_3 string,
   cvdcum_cvrt_err_dtl_x_3l string,
   cvdcum_cmd_actvy_d_3 varchar(150),
   cvdcum_cmd_rspns_actvy_d_3 varchar(150),
   cvdcum_src_c_3 varchar(15),
   cvdcum_tcu_strat_part_no_c_3 varchar(20),
   cvdcum_cmd_pk_x_3 varchar(64),
   cvdcum_suspct_f_3 boolean,
   cvdcun_created_on_s timestamp,
   cvdcun_created_by_c varchar(10)
)
partitioned by (cvdcum_partition_region_x string, cvdcum_partition_cntry_x string, cvdcum_partition_date_x string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/PaceTransformation/ncvdcum_pace_cmd_name_rspns_sec_hte'
tblproperties ('orc.compress.size'='8192');