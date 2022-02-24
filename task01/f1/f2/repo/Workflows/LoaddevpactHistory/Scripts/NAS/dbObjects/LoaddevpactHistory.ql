use ${hivevar:db_name};

--Master Table

create table if not exists NSCVPFY_tcx_SCHED_HIST_PII_HTE
(
   scvpfy_sha_k varchar(64),
   scvpfy_tcx_schedule_sha_k varchar(64),
   scvpfy_vin_d_3 Varchar (17),
   scvpfy_MKDactivescheduletimestamp_s_3 Timestamp,
   scvpfy_der_event_s_3 Timestamp,
   scvpfy_msg_metadata_msg_n String,
   scvpfy_created_on_s timestamp,
   scvpfy_created_by_c varchar(10)
)
partitioned by (scvpfy_partition_region_x string, scvpfy_partition_country_x string, scvpfy_partition_date_x string)
clustered by (scvpfy_vin_d_3) into 10 buckets
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvpfy_tcx_sched_hist_pii_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true','orc.compress.size'='8192');

--Staging Table
create external table if not exists NSCVSFY_tcx_HIST_ST_HTE
(
   sha_k varchar(64),
   vin Varchar (17),
   MKD_schedule_timestamp Timestamp,
   event_timestamp Timestamp,
   message_name String,
   function_msg_name String,
   partition_region String,
   partition_country String,
   partition_date String
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/nscvsfy_tcx_hist_st_hte'
TBLPROPERTIES ('orc.compress.size'='8192');

--Archive Table
create external table if not exists NSCVSFY_tcx_HIST_ARCHV_ST_HTE
(
   sha_key varchar(64),
   vin Varchar (17),
   MKDactivescheduletimestamp Timestamp,
   derived_event_timestamp Timestamp,
   msgmetadata_messagename String,
   functionmsgname String,
   partition_region_x String,
   partition_country_x String,
   partition_date_x String
)
partitioned by (created_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvsfy_tcx_hist_archv_st_hte'
TBLPROPERTIES ('orc.compress.size'='8192');

--Staging Temp Table for Inprocess location
create external table if not exists NSCVSFY_TEMP_tcx_HIST_ST_HTE
(
   sha_key varchar(64),
   vin Varchar (17),
   MKDactivescheduletimestamp Timestamp,
   derived_event_timestamp Timestamp,
   msgmetadata_messagename String,
   functionmsgname String,
   partition_region_x String,
   partition_country_x String,
   partition_date_x String
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/MKD/PostJob1/Inprocess/nscvsfy_temp_tcx_hist_st_hte';

