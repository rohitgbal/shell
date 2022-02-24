use ${hivevar:db_name};

 create external table if not exists NSCVC92_J9D_PJ1_SEC_HTE  
(
   scvc92_vin_d_3 varchar(17),
   scvc92_esn_d_3 varchar(15),
   scvc92_crltn_d_3 int,
   scvc92_app_crltn_d_3 varchar(150),
   scvc92_cmd_n_3 varchar(60),
   scvc92_cmd_s_3 timestamp,
   scvc92_cmd_stat_x_3 varchar(150),
   scvc92_cmd_rspns_s_3 timestamp,
   scvc92_durn_r_3 int,
   scvc92_wkup_sms_f_3 varchar(5),
   scvc92_cvrt_ver_c_3 varchar(20),
   scvc92_auth_stat_c_3 varchar(30),
   scvc92_anon_f_3 varchar(30),
   scvc92_cvrt_err_c_3 string,
   scvc92_cvrt_err_dtl_x_3l string,
   scvc92_cmd_actvy_d_3 varchar(150),
   scvc92_cmd_rspns_actvy_d_3 varchar(150),
   scvc92_src_c_3 varchar(15),
   scvc92_tcu_strat_part_no_c_3 varchar(20),
   scvc92_cmd_pk_x_3 varchar(64),
   scvc92_suspct_f_3 boolean,
   scvc92_created_on_s TIMESTAMP,
   scvc92_created_by_c VARCHAR(10)
)
partitioned by (scvc92_partition_region_x string, scvc92_partition_cntry_x string, scvc92_partition_date_x string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvc92_J9D_pj1_sec_hte'
tblproperties ('orc.compress.size'='8192');

create external table if not exists NSCVS92_J9D_PJ1_ST_HTE  
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
LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/nscvs92_J9D_pj1_st_hte'
TBLPROPERTIES ('orc.compress.size'='8192');