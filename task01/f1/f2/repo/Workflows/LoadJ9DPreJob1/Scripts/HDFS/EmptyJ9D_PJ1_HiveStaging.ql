use ${hivevar:db_name};
 
drop table if exists NSCVS92_J9D_PJ1_ST_HTE;

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
LOCATION '${hivevar:path_hdfs_staging_table}'
TBLPROPERTIES ('orc.compress.size'='8192');
